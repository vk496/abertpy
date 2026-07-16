import asyncio
import json
import re
import sys

import aiohttp
import backoff
import requests
from loguru import logger
from pydantic_typer import Typer

from abertpy import _HARDCODED_KEY
from abertpy.helpers import (
    extract_ppid_from_svcname,
    tvh_delete_svcs,
    tvh_find_overrides,
    tvh_get_muxes,
    tvh_get_svc_grid,
    tvh_get_svc_raw,
    tvh_get_svc_SID,
    tvh_set_mux_iptv_url,
)
from abertpy.models import ProxyArgs
from abertpy.setup import patch_original_SID_svc

app = Typer()

######################################
######################################
######################################
######################################


FRAME_SIZE = 188
MPEG_TS_START_BYTE = 0x47

AFC_PAYLOAD_ONLY = 0x10
AFC_ADAPTATION_PAYLOAD = 0x30


def process_data(packet: bytes, allowed_pid: int):
    if not packet or packet[0] != MPEG_TS_START_BYTE:
        logger.error("MPEG-TS first byte unexpected")
        return

    try:
        afc = packet[3] & 0x30
    except IndexError as e:
        logger.error(e)
        return

    tspid = ((packet[1] & 0x1F) << 8) | packet[2]
    if tspid != allowed_pid:
        return

    payload: bytes | None = None

    if afc == AFC_PAYLOAD_ONLY:
        payload = bytes(packet[4:])
    elif afc == AFC_ADAPTATION_PAYLOAD:
        payload = bytes(
            packet[packet[4] + 5 :]
        )  # 4 bytes header + 1 byte length indicator
    else:
        logger.error("AFC bad value {}", afc)
        return

    if payload:
        sys.stdout.buffer.write(payload)


async def recreate_mux_if_needed(arg: ProxyArgs) -> str | None:
    current_abertpy_mux = arg.service_uuid
    async with (
        aiohttp.ClientSession(
            raise_for_status=True,
            headers={
                "User-Agent": "curl/aiohttp"
            },  # https://docs.tvheadend.org/documentation/development/json-api/other-functions#play
        ) as session
    ):
        svc_overriden = await tvh_get_svc_raw(
            session=session,
            base_url=arg.get_base_url(),
            abertpy_ppid_uuid=current_abertpy_mux,
        )

        # Extract original SID
        name_abertpy_svc = svc_overriden.get("svcname", None)
        if not name_abertpy_svc or _HARDCODED_KEY not in name_abertpy_svc:
            raise ValueError(
                f"Cannot extract svcname from Abertis PPID service {current_abertpy_mux}"
            )

        match = re.search(r"\(SID:\s*(\w+)\)", name_abertpy_svc)
        if not match:
            raise ValueError(
                f"Cannot extract SID from service name: {name_abertpy_svc}"
            )

        original_sid: str = match.group(1)

        original_ppid = extract_ppid_from_svcname(name_abertpy_svc)
        if not original_ppid:
            raise ValueError(
                f"Cannot extract pPID from service name: {name_abertpy_svc}"
            )

        private_pid: int = int(svc_overriden.get("sid"))  # type: ignore

        # raw/export carries no mux reference, so resolve the transponder our
        # override lives on from the grid. Without it the SID lookup below could
        # match the same SID on a different transponder.
        parent_dvb_mux_uuid: str | None = next(
            (
                svc.get("multiplex_uuid", "")
                for svc in await tvh_get_svc_grid(
                    session, arg.get_base_url(), sid=private_pid
                )
                if svc.get("uuid", "") == current_abertpy_mux
            ),
            None,
        )

        # Get original Hispasat SVC from SID
        svc_hispasat_original = await tvh_get_svc_SID(
            session=session,
            base_url=arg.get_base_url(),
            original_sid=original_sid,
            mux_uuid=parent_dvb_mux_uuid,
        )

        # Validate if mux needs to be recreated
        if (
            svc_hispasat_original is None
            or svc_hispasat_original.get("uuid", "") == current_abertpy_mux
        ):
            new_mux_uuid = current_abertpy_mux
            logger.debug(
                "No need to recreate mux, already pointing to original service"
            )
        else:
            new_mux_uuid: str = svc_hispasat_original.get("uuid", "")

            # Mux was changed. Recreate it
            logger.warning("Recreating Abertis mux to point to original service")

            # Obtain the RAW one

            svc_hispasat_raw = await tvh_get_svc_raw(
                session=session,
                base_url=arg.get_base_url(),
                abertpy_ppid_uuid=svc_hispasat_original["uuid"],
            )

            patch_original_SID_svc(svc_hispasat_raw, private_pid, original_sid)

            async with session.post(
                arg.get_base_url() + "/api/raw/import",
                data={
                    "node": json.dumps(svc_hispasat_raw),
                },
            ) as response:
                pass

            # Reap every override this one replaces, not just the uuid the mux
            # happened to name: the import above has just taken over the node we
            # now stream from, so anything else on this pPID is dead weight.
            if parent_dvb_mux_uuid:
                stale = [
                    svc["uuid"]
                    for svc in await tvh_find_overrides(
                        session, arg.get_base_url(), parent_dvb_mux_uuid, private_pid
                    )
                    if svc["uuid"] != new_mux_uuid
                ]
            else:
                stale = [svc_overriden["uuid"]]

            deleted = await tvh_delete_svcs(session, arg.get_base_url(), stale)
            logger.info("pPID {}: reaped {} stale override(s)", private_pid, deleted)

        # Update mux references (iptv_url) to the new UUID of the service
        all_muxes: list = (await tvh_get_muxes(session, arg.get_base_url())).get(
            "entries", []
        )

        dvb_mux_name: str = next(
            (
                mux.get("name", "")
                for mux in all_muxes
                if mux["uuid"] == parent_dvb_mux_uuid
            ),
            "",
        )
        target_muxname = (
            f"{_HARDCODED_KEY}: MUX {dvb_mux_name} pPID {original_ppid}"
            if dvb_mux_name
            else ""
        )

        # More than one mux can share a service: an early scan of the wrong
        # transponder left muxes named for one and fed by another, and those are
        # the ones carrying the channel mappings. Repointing only the
        # canonically-named one would strand the rest on the service just reaped,
        # so fix every mux that fed off this pPID, keyed on the uuid it holds.
        # A pPID that genuinely repeats on another transponder has its own
        # override, is absent from `orphaned`, and is left alone.
        orphaned: set[str] = set(stale) | {current_abertpy_mux}

        updated = 0
        for mux in all_muxes:
            iptv_url: str = mux.get("iptv_url", "")
            if not iptv_url or new_mux_uuid in iptv_url:
                continue

            target = re.search(r"[a-fA-F0-9]{32}", iptv_url)
            target_uuid = target.group(0) if target else ""

            # Either it points at a service we just retired, or it is the mux
            # this pPID is named for and has drifted (e.g. dangling from a run
            # that only fixed its twin).
            if not (
                target_uuid in orphaned
                or (target_muxname and mux.get("iptv_muxname", "") == target_muxname)
            ):
                continue

            # Swap the uuid we know is in there rather than the first 32 hex
            # chars anywhere, which a custom --pipe-command could well hold.
            new_iptv_url = (
                iptv_url.replace(target_uuid, new_mux_uuid)
                if target_uuid
                else re.sub(r"[a-fA-F0-9]{32}", new_mux_uuid, iptv_url, count=1)
            )

            await tvh_set_mux_iptv_url(
                session, arg.get_base_url(), mux["uuid"], new_iptv_url
            )
            logger.warning(
                "MUX {} didn't point to the correct service, updated it.",
                mux.get("iptv_muxname", mux["uuid"]),
            )
            updated += 1

        if not updated:
            return None

        return new_mux_uuid


# requests raises its own ConnectionError, a sibling of the builtin rather than a
# subclass, so naming only the builtin here would never retry anything. Repointing
# a mux makes TVheadend restart it and drop the subscription we are about to read,
# so the first attempt tearing down is normal: the retry streams from the mux we
# have just corrected.
@backoff.on_exception(
    backoff.expo,
    (ConnectionError, requests.exceptions.ConnectionError),
    max_time=10,
)
@app.command(help="Proxy for Abertis streams")
def proxy(arg: ProxyArgs):

    base_url = arg.get_base_url()

    new_svc_uuid = asyncio.run(recreate_mux_if_needed(arg))

    if new_svc_uuid:
        # Was corrected
        endpoint = f"{base_url}/stream/service/{new_svc_uuid}"
    else:
        endpoint = f"{base_url}/stream/service/{arg.service_uuid}"

    response = requests.get(
        endpoint, stream=True, headers={"User-Agent": "curl/aiohttp"}
    )
    for packet in response.iter_content(chunk_size=FRAME_SIZE):
        process_data(packet=packet, allowed_pid=arg.allowed_pid)
