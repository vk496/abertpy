import asyncio
import json
import re
import sys
import time
from collections.abc import Iterator

import aiohttp
import backoff
import requests
import typer
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


def extract_payload(packet: bytes, allowed_pid: int) -> bytes | None:
    """The elementary-stream payload of one 188-byte TS frame, or None if it
    doesn't belong to allowed_pid (or isn't a valid frame)."""
    if not packet or packet[0] != MPEG_TS_START_BYTE:
        logger.error("MPEG-TS first byte unexpected")
        return None

    try:
        afc = packet[3] & 0x30
    except IndexError as e:
        logger.error(e)
        return None

    tspid = ((packet[1] & 0x1F) << 8) | packet[2]
    if tspid != allowed_pid:
        return None

    if afc == AFC_PAYLOAD_ONLY:
        return packet[4:]
    elif afc == AFC_ADAPTATION_PAYLOAD:
        return packet[packet[4] + 5 :]  # 4 bytes header + 1 byte length indicator
    else:
        logger.error("AFC bad value {}", afc)
        return None


# The actual HTTP read() size feeding the batching below -- deliberately
# much smaller than the configured batch size. Reading in small increments
# (instead of asking iter_content for the full batch directly) is what lets
# a batch flush on a bounded latency even for a pPID too low-bitrate to fill
# the target size quickly: iter_content(chunk_size=N) blocks until N bytes
# have arrived, so for a low-bitrate pPID, asking it for the whole configured
# batch size directly can mean seconds of added latency -- measured against a
# real captured 30s/20Mbps channel resampled down to 235kbps: batches arrived
# in ~2.2s bursts without this, vs ~0.5s with it. The extra iter_content()
# calls this costs measured as ~12-15% more CPU at 20Mbps than reading the
# full batch size directly in one call -- worth it for the latency bound.
_UNDERLYING_READ_BYTES = 4096

# Upper bound on how long unflushed bytes sit before being written out, even
# if the configured batch size hasn't been reached. This is what actually
# bounds the latency described above; _UNDERLYING_READ_BYTES only exists to
# let this be checked often enough to matter.
_MAX_BATCH_LATENCY_S = 0.5


def iter_batches(response: requests.Response, read_chunk_log2: int) -> Iterator[bytes]:
    """FRAME_SIZE-aligned chunks of raw bytes from a streaming response,
    flushed once 2**read_chunk_log2 bytes accumulate or
    _MAX_BATCH_LATENCY_S has passed since the last flush, whichever comes
    first.

    Reading and writing one 188-byte TS frame at a time (as this used to do)
    measured ~5x more CPU than batching against a real captured 30s/20Mbps
    channel -- at ~13k packets/sec, requests/urllib3's per-call overhead and
    the per-call write() overhead both dominated, well before the actual TS
    parsing became the bottleneck. 14-20 all performed well in that
    measurement; returns flatten and then reverse past roughly 22, from
    larger buffer allocation/copy overhead outweighing the saved call count.
    """
    read_chunk_bytes = 2**read_chunk_log2
    underlying_read_bytes = min(_UNDERLYING_READ_BYTES, read_chunk_bytes)

    buf = bytearray()
    last_flush = time.monotonic()
    for chunk in response.iter_content(chunk_size=underlying_read_bytes):
        buf += chunk
        now = time.monotonic()
        if len(buf) >= read_chunk_bytes or now - last_flush >= _MAX_BATCH_LATENCY_S:
            aligned_len = (len(buf) // FRAME_SIZE) * FRAME_SIZE
            if aligned_len:
                batch = bytes(buf[:aligned_len])
                del buf[:aligned_len]
                yield batch
            last_flush = now


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

        # Fetched early (and reused below) so the single summary log line at
        # the end can name this pPID the same way TVheadend's own UI does,
        # e.g. "abertpy: MUX 11653H pPID 303".
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
        # target_muxname itself must stay "" when unresolved, since it's also
        # matched against mux names below; this is purely for the log lines.
        mux_label = target_muxname or f"pPID {original_ppid}"

        # Get original Hispasat SVC from SID
        svc_hispasat_original = await tvh_get_svc_SID(
            session=session,
            base_url=arg.get_base_url(),
            original_sid=original_sid,
            mux_uuid=parent_dvb_mux_uuid,
        )

        # Validate if mux needs to be recreated
        stale: list[str] = []
        recreated = False
        deleted = 0
        if (
            svc_hispasat_original is None
            or svc_hispasat_original.get("uuid", "") == current_abertpy_mux
        ):
            new_mux_uuid = current_abertpy_mux
        else:
            new_mux_uuid: str = svc_hispasat_original.get("uuid", "")
            recreated = True

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
            ):
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

        # More than one mux can share a service: an early scan of the wrong
        # transponder left muxes named for one and fed by another, and those are
        # the ones carrying the channel mappings. Repointing only the
        # canonically-named one would strand the rest on the service just reaped,
        # so fix every mux that fed off this pPID, keyed on the uuid it holds.
        # A pPID that genuinely repeats on another transponder has its own
        # override, is absent from `orphaned`, and is left alone.
        orphaned: set[str] = set(stale) | {current_abertpy_mux}

        updated = 0
        touched_mux_uuids: list[str] = []
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
            updated += 1
            touched_mux_uuids.append(mux["uuid"])

        if recreated or updated:
            # Best-effort: the mux we just repointed is where TVheadend scans
            # a real playable service (and the viewer-facing channel name,
            # usually identical) from, e.g. "La 1 UHD" -- much more useful
            # here than the internal pPID/transponder label alone.
            channel_names: dict[str, None] = {}
            for mux_uuid in touched_mux_uuids:
                for svc in await tvh_get_svc_grid(
                    session, arg.get_base_url(), multiplex_uuid=mux_uuid
                ):
                    name = svc.get("svcname")
                    if name:
                        channel_names[name] = None
            label = (
                f"{mux_label} ({', '.join(channel_names)})"
                if channel_names
                else mux_label
            )

            details = []
            if recreated:
                details.append(f"recreated (reaped {deleted} stale override(s))")
            if updated:
                details.append(f"repointed {updated} mux(es) still on the old service")
            logger.warning("{}: {}", label, "; ".join(details))
        else:
            logger.debug("{}: already correct, nothing to do", mux_label)

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
def _stream(arg: ProxyArgs) -> None:
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
    for batch in iter_batches(response, arg.read_chunk_log2):
        out = bytearray()
        for offset in range(0, len(batch), FRAME_SIZE):
            payload = extract_payload(
                batch[offset : offset + FRAME_SIZE], arg.allowed_pid
            )
            if payload:
                out += payload
        if out:
            sys.stdout.buffer.write(out)


@app.command(help="Proxy for Abertis streams")
def proxy(arg: ProxyArgs):
    try:
        _stream(arg)
    except (ConnectionError, requests.exceptions.ConnectionError) as e:
        # TVheadend drops our subscription whenever it repoints or rescans the
        # underlying mux; the retries above already cover that case. Getting
        # here means the connection kept failing for the whole retry window
        # (e.g. TVheadend itself is unavailable) -- TVheadend will spawn us
        # again once the channel is next needed, so there's nothing to do but
        # say why we stopped, not dump a full traceback for an expected,
        # already-retried condition.
        logger.warning("Giving up on service {}: {}", arg.service_uuid, e)
        raise typer.Exit(1)
