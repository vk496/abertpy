import json
import re

import aiohttp
from loguru import logger

from abertpy import _HARDCODED_KEY, _HARDCODED_PMT

# TVheadend's grid API returns only the first 50 rows when no limit is given, and
# still reports the full match count in "total", so an unlimited query silently
# reads a partial list. Every grid call must pass a limit explicitly.
_GRID_LIMIT = 99999


async def tvh_get_networks(session: aiohttp.ClientSession, base_url: str):
    networks_url = base_url + "/api/mpegts/network/grid"
    async with session.post(
        networks_url,
        data={
            "limit": _GRID_LIMIT,
        },
    ) as response:
        networks: dict = await response.json()
        return networks


async def tvh_get_muxes(session: aiohttp.ClientSession, base_url: str):
    networks_url = base_url + "/api/mpegts/mux/grid"
    async with session.post(
        networks_url,
        data={
            "limit": _GRID_LIMIT,
        },
    ) as response:
        muxes: dict = await response.json()
        return muxes


async def tvh_get_svc_grid(
    session: aiohttp.ClientSession,
    base_url: str,
    *,
    sid: int | str | None = None,
    svcname: str | None = None,
    multiplex_uuid: str | None = None,
) -> list[dict]:
    """Query the service grid, narrowing server-side and always with a limit."""
    filters: list[dict] = []
    if sid is not None:
        filters.append(
            {"type": "numeric", "comparison": "eq", "field": "sid", "value": int(sid)}
        )
    if svcname is not None:
        filters.append({"type": "string", "field": "svcname", "value": svcname})
    if multiplex_uuid is not None:
        filters.append(
            {"type": "string", "field": "multiplex_uuid", "value": multiplex_uuid}
        )

    data: dict = {"hidemode": "none", "limit": _GRID_LIMIT}
    if filters:
        data["filter"] = json.dumps(filters)

    async with session.post(
        f"{base_url}/api/mpegts/service/grid", data=data
    ) as response:
        resp = await response.json()

    entries: list[dict] = resp.get("entries", [])
    total: int = resp.get("total", len(entries))
    if total > len(entries):
        logger.warning(
            "Service grid returned {} of {} matches; the list is truncated",
            len(entries),
            total,
        )

    return entries


def is_abertpy_svc(service: dict) -> bool:
    return _HARDCODED_KEY in service.get("svcname", "")


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
    session: aiohttp.ClientSession,
    base_url: str,
    original_sid: str | int,
    mux_uuid: str | None = None,
) -> dict | None:
    """The TVheadend-owned service carrying this SID, never one of our overrides.

    An override reuses the pPID as its sid, so a bare sid lookup can return one
    of ours whenever a pPID happens to collide with a real SID. The same SID can
    also repeat across transponders, hence the mux filter.
    """
    entries = [
        svc
        for svc in await tvh_get_svc_grid(session, base_url, sid=original_sid)
        if not is_abertpy_svc(svc)
        and (mux_uuid is None or svc.get("multiplex_uuid", "") == mux_uuid)
    ]

    if not entries:
        return None

    return entries[0]


async def tvh_find_overrides(
    session: aiohttp.ClientSession, base_url: str, mux_uuid: str, private_pid: int
) -> list[dict]:
    """Our override services for this pPID on this mux, best candidate first.

    An enabled one wins: TVheadend disables an override once a scan notices the
    pPID is not a real SID in the PAT, and a disabled service refuses to stream.
    Ties break on the newest, which is the most recently hijacked node.
    """
    overrides = [
        svc
        for svc in await tvh_get_svc_grid(session, base_url, sid=private_pid)
        if is_abertpy_svc(svc) and svc.get("multiplex_uuid", "") == mux_uuid
    ]

    overrides.sort(
        key=lambda svc: (bool(svc.get("enabled")), svc.get("created", 0)),
        reverse=True,
    )

    return overrides


async def tvh_svc_mux_name(
    session: aiohttp.ClientSession, base_url: str, svc_uuid: str, sid: int
) -> str:
    """Name of the transponder a service actually sits on, "" if not found."""
    for svc in await tvh_get_svc_grid(session, base_url, sid=sid):
        if svc.get("uuid", "") == svc_uuid:
            return svc.get("multiplex", "")

    return ""


async def tvh_delete_svcs(
    session: aiohttp.ClientSession, base_url: str, uuids: list[str]
) -> int:
    deleted = 0
    for uuid in uuids:
        try:
            async with session.post(
                f"{base_url}/api/idnode/delete",
                data={
                    "uuid": uuid,
                },
            ):
                pass
        except aiohttp.ClientResponseError as e:
            # 404 means someone already removed it, which is the outcome we want
            if e.status != 404:
                raise
            logger.debug(f"Service {uuid} was already gone")
            continue

        deleted += 1

    return deleted


async def tvh_set_mux_iptv_url(
    session: aiohttp.ClientSession, base_url: str, mux_uuid: str, iptv_url: str
) -> None:
    """Point a mux at a different service, leaving the rest of its config alone."""
    async with session.post(
        f"{base_url}/api/idnode/load",
        data={
            "uuid": mux_uuid,
        },
    ) as response:
        resp = await response.json()

    entries: list = resp.get("entries", [])
    if not entries:
        raise ValueError(f"Cannot load mux {mux_uuid}")

    node = {param["id"]: param.get("value") for param in entries[0].get("params", [])}
    if "iptv_url" not in node:
        raise ValueError(f"Cannot find iptv_url param in mux {mux_uuid}")

    node["iptv_url"] = iptv_url
    node["uuid"] = mux_uuid

    async with session.post(
        f"{base_url}/api/idnode/save",
        data={
            "node": json.dumps(node),
        },
    ):
        pass


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


def extract_ppid_from_svcname(svcname: str) -> int | None:
    """
    Extract the pPID from the svcname string.
    Example svcname: "Abertis: raw pPID 1234 (SID: 5678)"
    """
    match = re.search(r"pPID\s*(\w+)", svcname)
    if not match:
        return None

    return int(match.group(1))
