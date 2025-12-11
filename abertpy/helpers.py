import json

import aiohttp
from loguru import logger

from abertpy import _HARDCODED_KEY, _HARDCODED_PMT


async def tvh_get_networks(session: aiohttp.ClientSession, base_url: str):
    networks_url = base_url + "/api/mpegts/network/grid"
    async with session.post(
        networks_url,
        data={
            "limit": 99999,
        },
    ) as response:
        networks: dict = await response.json()
        return networks


async def tvh_get_muxes(session: aiohttp.ClientSession, base_url: str):
    networks_url = base_url + "/api/mpegts/mux/grid"
    async with session.post(
        networks_url,
        data={
            "limit": 99999,
        },
    ) as response:
        muxes: dict = await response.json()
        return muxes


async def tvh_get_svc_raw(
    session: aiohttp.ClientSession, base_url: str, abertpy_ppid_uuid: str
) -> dict:

    # First, we get the current name of the SVC
    async with session.get(
        f"{base_url}/api/raw/export",
        params={
            "uuid": abertpy_ppid_uuid,
        },
    ) as response:
        resp = await response.json()

    if not resp:
        raise ValueError(f"Overriden SVC not exist with UUID {abertpy_ppid_uuid}")

    return resp[0]


async def tvh_get_svc_SID(
    session: aiohttp.ClientSession, base_url: str, original_sid: str
) -> dict | None:

    async with session.post(
        f"{base_url}/api/mpegts/service/grid",
        data={
            "hidemode": "none",
            "filter": json.dumps(
                [
                    {
                        "type": "numeric",
                        "comparison": "eq",
                        "field": "sid",
                        "value": original_sid,
                    }
                ]
            ),
        },
    ) as response:
        resp = await response.json()

    svcs: list = resp.get("entries", [])

    if not svcs:
        return None

    return svcs[0]


def patch_original_SID_svc(sid_original: dict, private_pid: int, service_sid: str):

    logger.debug(f"Original SID data: {sid_original}")

    sid_original["stream"].append(
        {
            "pid": 8191,
            "type": "CA",
            "position": 0,
            "caidlist": [{"caid": 9728}, {"caid": 9728}],
        }
    )

    # Hijack Tvheadend
    sid_original["sid"] = private_pid
    sid_original["verified"] = (
        1  # Very important ortherwise tvheadend will not stream data
    )
    sid_original["pmt"] = _HARDCODED_PMT  # Always 8000?
    sid_original["stream"].append({"type": "H264", "position": 0, "pid": private_pid})
    sid_original["svcname"] = (
        f"{_HARDCODED_KEY}: raw pPID {private_pid} (SID: {service_sid})"  # pd stands for private data
    )
    sid_original["enabled"] = True
