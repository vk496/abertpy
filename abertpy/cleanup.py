import asyncio
import re
from collections import defaultdict

import aiohttp
from loguru import logger
from pydantic_typer import Typer

from abertpy import _HARDCODED_KEY
from abertpy.helpers import (
    is_abertpy_svc,
    tvh_delete_svcs,
    tvh_get_muxes,
    tvh_get_svc_grid,
    tvh_set_mux_iptv_url,
)
from abertpy.models import CleanupArgs

app = Typer()

# "abertpy: MUX 11222H pPID 2060" -> transponder name + pPID
_MUXNAME_RE = re.compile(rf"^{re.escape(_HARDCODED_KEY)}: MUX (\S+) pPID (\d+)$")


def _best_first(overrides: list[dict]) -> list[dict]:
    """Same ranking as tvh_find_overrides: usable first, then newest."""
    return sorted(
        overrides,
        key=lambda svc: (bool(svc.get("enabled")), svc.get("created", 0)),
        reverse=True,
    )


async def cleanup_async(arg: CleanupArgs) -> None:
    async with aiohttp.ClientSession(
        raise_for_status=True,
        headers={"User-Agent": "curl/aiohttp"},
    ) as session:
        base_url = arg.get_base_url()

        overrides = [
            svc
            for svc in await tvh_get_svc_grid(session, base_url, svcname=_HARDCODED_KEY)
            if is_abertpy_svc(svc)
        ]

        # An override is identified by the transponder it lives on plus its pPID,
        # which it stores as its sid. Anything else in a group is a duplicate.
        groups: dict[tuple[str, int], list[dict]] = defaultdict(list)
        for svc in overrides:
            groups[(svc.get("multiplex_uuid", ""), svc.get("sid", -1))].append(svc)

        ranked_groups = {key: _best_first(svcs) for key, svcs in groups.items()}
        keep: dict[tuple[str, int], dict] = {
            key: ranked[0] for key, ranked in ranked_groups.items()
        }

        svc_by_uuid: dict[str, dict] = {svc["uuid"]: svc for svc in overrides}

        muxes: list = (await tvh_get_muxes(session, base_url)).get("entries", [])
        dvb_uuid_by_name: dict[str, str] = {
            mux.get("name", ""): mux["uuid"]
            for mux in muxes
            if not mux.get("iptv_muxname", "")
        }

        # A mux we are about to strip of its service has to be repointed at the
        # survivor first, or playback breaks until the next scan.
        repoint: list[tuple[dict, str]] = []
        protected: set[str] = set()
        for mux in muxes:
            muxname: str = mux.get("iptv_muxname", "")
            match = _MUXNAME_RE.match(muxname)
            if not match:
                continue

            mux_freq, private_pid = match.group(1), int(match.group(2))
            iptv_url: str = mux.get("iptv_url", "")
            target = re.search(r"[a-fA-F0-9]{32}", iptv_url)
            target_uuid: str = target.group(0) if target else ""

            # The service a mux points at names its own group. Trust that over
            # the mux name, which an early scan of the wrong transponder could
            # have got wrong: some muxes say 11302H over services on 12548V.
            current = svc_by_uuid.get(target_uuid)
            key = (
                (current.get("multiplex_uuid", ""), current.get("sid", -1))
                if current is not None
                else (dvb_uuid_by_name.get(mux_freq, ""), private_pid)
            )

            survivor = keep.get(key)
            if survivor is None or not target_uuid:
                logger.warning("Mux {} has no surviving service, leaving alone", muxname)
                # Whatever it still streams from has to outlive this cleanup
                protected.add(target_uuid)
                continue

            if survivor["uuid"] != target_uuid:
                repoint.append((mux, iptv_url.replace(target_uuid, survivor["uuid"])))

        stale: list[dict] = [
            svc
            for ranked in ranked_groups.values()
            for svc in ranked[1:]
            if svc["uuid"] not in protected
        ]

        logger.info(
            "{} abertpy service(s) over {} pPID(s): keeping {}, {} stale, "
            "{} mux(es) to repoint",
            len(overrides),
            len(groups),
            len(keep),
            len(stale),
            len(repoint),
        )

        if not arg.apply:
            for mux, new_iptv_url in repoint:
                logger.info("would repoint {}: {}", mux["iptv_muxname"], new_iptv_url)
            logger.warning(
                "Dry run: would delete {} service(s) and repoint {} mux(es). "
                "Re-run with --apply to do it.",
                len(stale),
                len(repoint),
            )
            return

        for mux, new_iptv_url in repoint:
            await tvh_set_mux_iptv_url(session, base_url, mux["uuid"], new_iptv_url)
            logger.info("Repointed {}", mux["iptv_muxname"])

        deleted = await tvh_delete_svcs(session, base_url, [svc["uuid"] for svc in stale])
        logger.info("Deleted {} stale service(s)", deleted)


@app.command(help="Remove duplicate abertpy services left by earlier runs")
def cleanup(arg: CleanupArgs):
    return asyncio.run(cleanup_async(arg))
