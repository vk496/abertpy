import asyncio
import json
import re
import sys
from venv import logger

import aiohttp
import backoff
import requests
from pydantic_typer import Typer

from abertpy import _HARDCODED_KEY
from abertpy.helpers import extract_ppid_from_svcname, tvh_get_svc_raw, tvh_get_svc_SID
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
    async with aiohttp.ClientSession(
        raise_for_status=True,
        headers={
            "User-Agent": "curl/aiohttp"
        },  # https://docs.tvheadend.org/documentation/development/json-api/other-functions#play
    ) as session:

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

        # Get original Hispasat SVC from SID
        svc_hispasat_original = await tvh_get_svc_SID(
            session=session,
            base_url=arg.get_base_url(),
            original_sid=original_sid,
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

            private_pid: int = int(svc_overriden.get("sid"))  # type: ignore

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

            # Delete old SVC
            svc_overriden_old_uuid = svc_overriden["uuid"]

            async with session.post(
                arg.get_base_url() + "/api/idnode/delete",
                data={
                    "uuid": svc_overriden_old_uuid,
                },
            ) as response:
                resp = await response.json()

        # Update mux reference (iptv_url) to the new UUID of the service if needed
        async with session.post(
            f"{arg.get_base_url()}/api/mpegts/mux/grid",
            data={
                "hidemode": "none",
                "filter": json.dumps(
                    [
                        {
                            "type": "string",
                            "field": "name",
                            "value": f"pPID {original_ppid}",
                        }
                    ]
                ),
            },
        ) as response:
            resp = await response.json()

        svcs: list = resp.get("entries", [])
        if not svcs:
            raise ValueError(f"Cannot find Abertis PPID mux")

        parent_mux_uuid = svcs[0].get("uuid")

        async with session.post(
            f"{arg.get_base_url()}/api/idnode/load",
            data={
                "uuid": parent_mux_uuid,
            },
        ) as response:
            resp = await response.json()

        load_mux: list = resp.get("entries", [])
        if not load_mux:
            raise ValueError(f"Cannot load Abertis PPID mux")

        mux_loaded_data = load_mux[0]

        new_mux_data = {}

        found_iptv_url = False
        for mux_param in mux_loaded_data.get("params", []):
            new_mux_data[mux_param.get("id")] = mux_param.get("value")

            if mux_param.get("id") == "iptv_url":
                found_iptv_url = True

        if not found_iptv_url:
            raise ValueError(f"Cannot find iptv_url param in mux data")

        # Add missing uuid field
        new_mux_data["uuid"] = parent_mux_uuid

        if new_mux_uuid not in new_mux_data["iptv_url"]:
            logger.warning("MUX doesn't point to the correct service, updating it.")

            new_mux_data["iptv_url"] = re.sub(
                r"[a-fA-F0-9]{32}", new_mux_uuid, new_mux_data["iptv_url"], count=1
            )

            async with session.post(
                f"{arg.get_base_url()}/api/idnode/save",
                data={
                    "node": json.dumps(new_mux_data),
                },
            ) as response:
                resp = await response.json()

            return new_mux_uuid


@backoff.on_exception(backoff.expo, ConnectionError, max_time=10)
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
