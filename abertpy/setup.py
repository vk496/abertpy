import asyncio
import json
import subprocess

import aiohttp
from loguru import logger
from pydantic_typer import Typer

from abertpy.helpers import tvh_get_muxes, tvh_get_networks
from abertpy.models import SetupArgs

app = Typer()

_HARDCODED_KEY = "abertpy"
_HARDCODED_PMT = 8000


_MAP_PPID_CA: dict[int, int] = {}


async def get_muxes(session: aiohttp.ClientSession, arg: SetupArgs) -> list[dict]:

    muxes = await tvh_get_muxes(session, arg.get_base_url())

    target_muxes: list[dict] = [
        mux
        for mux in muxes.get("entries", [])
        if mux.get("enabled", True)
        and mux.get("network_uuid", None) == arg.network_uuid
    ]

    logger.info(f"Target muxes: {len(target_muxes)}")
    return target_muxes


async def fetch_mux_data(
    session: aiohttp.ClientSession,
    url: str,
    arg: SetupArgs,
    buffer: bytearray,
):
    async with session.get(url) as response:
        total_bytes = 0
        async for data in response.content.iter_chunked(1024 * 10):
            total_bytes += len(data)
            buffer.extend(data)
            if total_bytes >= arg.mux_buffer_size:
                logger.debug(f"Stopped after receiving {total_bytes} bytes.")
                return


async def get_mux_data(
    session: aiohttp.ClientSession,
    arg: SetupArgs,
    url: str,
):
    buffer: bytearray = bytearray()
    try:
        await asyncio.wait_for(
            fetch_mux_data(session, url, arg, buffer),
            timeout=arg.mux_buffer_time.total_seconds(),
        )
    except asyncio.TimeoutError:
        pass

    process = subprocess.Popen(
        [str(arg.tsanalyze_path), "--json"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    stdout, _ = process.communicate(input=buffer)
    tsanlyze_output = json.loads(stdout)

    return tsanlyze_output


async def create_iptv_network(session: aiohttp.ClientSession, arg: SetupArgs) -> str:

    # Find network with pnetworkname containing abertpy
    networks = await tvh_get_networks(session, arg.get_base_url())

    for network in networks.get("entries", []):
        if _HARDCODED_KEY in network.get("networkname", ""):
            logger.info(f"IPTV Network already exist: {network.get('networkname')}")
            return network.get("uuid")

    async with session.post(
        arg.get_base_url() + "/api/mpegts/network/create",
        data={
            "class": "iptv_network",
            "conf": json.dumps(
                {
                    "enabled": True,
                    "networkname": f"{_HARDCODED_KEY}: Abertis",  # Keyword to find it later
                    "bouquet": False,
                    "max_streams": 0,
                    "max_bandwidth": 0,
                    "pnetworkname": "",
                    "nid": 0,
                    "ignore_chnum": False,
                    "satip_source": 0,
                    "charset": "",
                    "use_libav": False,
                    "scan_create": True,
                    "priority": 1,
                    "spriority": 1,
                    "max_timeout": 15,
                    "icon_url": "",
                    "skipinitscan": True,
                    "idlescan": False,
                    "sid_chnum": False,
                    "localtime": 0,
                    "service_sid": 0,
                    "remove_scrambled": True,
                }
            ),
        },
    ) as response:
        net_uuid: dict[str, str] = await response.json()
        return net_uuid["uuid"]


async def tvh_find_service_uuid(
    session: aiohttp.ClientSession,
    arg: SetupArgs,
    mux_uuid: str,
    service_sid: int,
) -> str:
    async with session.post(
        arg.get_base_url() + "/api/mpegts/service/grid",
        data={
            "hidemode": "none",
            "filter": json.dumps(
                [
                    {
                        "type": "numeric",
                        "comparison": "eq",
                        "field": "sid",
                        "value": service_sid,
                    }
                ]
            ),
        },
    ) as response:
        resp = await response.json()

    for service in resp.get("entries", []):
        if service.get("multiplex_uuid", "") == mux_uuid:
            return service["uuid"]

    raise ValueError(f"Service UUID not found for SID {service_sid} in mux {mux_uuid}")


async def tvh_override_exists(
    session: aiohttp.ClientSession,
    arg: SetupArgs,
    mux_uuid: str,
    private_pid: int,
) -> str | None:
    async with session.post(
        arg.get_base_url() + "/api/mpegts/service/grid",
        data={
            "hidemode": "none",
            "filter": json.dumps(
                [
                    {
                        "type": "string",
                        "field": "svcname",
                        "value": _HARDCODED_KEY,
                    }
                ]
            ),
        },
    ) as response:
        resp = await response.json()

    for service in resp.get("entries", []):
        if (
            service.get("multiplex_uuid", "") == mux_uuid
            and service.get("sid", -1) == private_pid
            and _HARDCODED_KEY in service.get("svcname", "")
        ):
            return service["uuid"]


async def recreate_tvh_service(
    session: aiohttp.ClientSession,
    arg: SetupArgs,
    mux_uuid: str,
    private_pid: int,
    service_sid: int,
) -> str:

    svc_uuid: str | None = None

    # Extract original SID always.
    tvh_original_uuid: str = await tvh_find_service_uuid(
        session, arg, mux_uuid, service_sid
    )
    tvh_overriden_uuid: str | None = await tvh_override_exists(
        session, arg, mux_uuid, private_pid
    )

    svc_uuid = tvh_overriden_uuid or tvh_original_uuid

    async with session.get(
        arg.get_base_url() + "/api/raw/export",
        params={
            "uuid": svc_uuid,
        },
    ) as response:
        res = await response.json()

    sid_original = (res)[0]

    if pcr := sid_original.get("pcr", None):
        _MAP_PPID_CA[private_pid] = pcr

    # Avoid duplicates
    if tvh_overriden_uuid:
        return tvh_overriden_uuid

    logger.debug(f"Original SID data: {sid_original}")

    # Hijack Tvheadend
    sid_original["sid"] = private_pid
    sid_original["verified"] = (
        1  # Very important ortherwise tvheadend will not stream data
    )
    sid_original["pmt"] = _HARDCODED_PMT  # Always 8000?
    sid_original["stream"].append({"type": "H264", "position": 0, "pid": private_pid})
    sid_original["svcname"] = (
        f"{_HARDCODED_KEY}: PID {private_pid} (SID: {service_sid})"  # pd stands for private data
    )
    sid_original["enabled"] = True

    async with session.post(
        arg.get_base_url() + "/api/raw/import",
        data={
            "node": json.dumps(sid_original),
        },
    ) as response:
        pass

    return tvh_original_uuid


async def recreate_tvh_iptv_mux(
    session: aiohttp.ClientSession,
    arg: SetupArgs,
    iptv_network_uuid: str,
    svc_mux_uuid: str,
    private_pid: int,
):

    muxes = await tvh_get_muxes(session, arg.get_base_url())

    for mux in muxes.get("entries", []):
        if (
            mux.get("network_uuid", None) == iptv_network_uuid
            and _HARDCODED_KEY in mux.get("iptv_muxname", "")
            and f"{private_pid}" in mux.get("iptv_muxname", "")
        ):
            return

    async with session.post(
        arg.get_base_url() + "/api/mpegts/network/mux_create",
        data={
            "uuid": iptv_network_uuid,
            "conf": json.dumps(
                {
                    "enabled": 1,
                    "epg": 1,
                    "epg_module_id": "",
                    "iptv_url": arg.get_iptv_pipe(svc_mux_uuid=svc_mux_uuid),
                    "use_libav": 0,
                    "iptv_atsc": False,
                    "iptv_muxname": f"{_HARDCODED_KEY}: pd PID {private_pid}",
                    "channel_number": "0",
                    "iptv_sname": "",
                }
            ),
        },
    ) as response:
        pass


######################################
######################################
######################################
######################################


async def setup_async(arg: SetupArgs):
    base_url = arg.get_base_url()

    async with aiohttp.ClientSession(
        raise_for_status=True,
        headers={
            "User-Agent": "curl/aiohttp"
        },  # https://docs.tvheadend.org/documentation/development/json-api/other-functions#play
    ) as session:

        # First thing, create a IPTV Network if not existing
        abertis_net_uuid = await create_iptv_network(session, arg)

        # Get enabled muxes from tvheadend
        list_muxes = await get_muxes(session, arg)

        map_dataPID_SID: dict[int, int] = {}

        for mux in list_muxes:
            mux_uuid = mux["uuid"]
            mux_freq: str = mux.get("name", "")

            logger.debug(f"Scanning mux: {mux_uuid} - {mux_freq}")
            tsanalyzer_dict = await get_mux_data(
                session,
                arg,
                f"{base_url}/play/ticket/stream/mux/{mux.get('uuid')}",
            )

            for pid in tsanalyzer_dict.get("pids", []):
                # Skip PMT
                if pid["pmt"]:
                    continue

                # Skip FTA
                if not pid["is-scrambled"]:
                    continue

                # Skip audio/video
                if pid["audio"] or pid["video"]:
                    continue

                # No lang
                if pid.get("language", None):
                    continue

                # Must have sercvices. TODO: maybe more than 1 svc?
                if pid["service-count"] != 1:
                    continue

                logger.debug(f"PID: {pid}")

                # Associate Private data PID to SID
                abertis_data_pid = pid["id"]
                service_sid = pid["services"][0]

                svc_mux_uuid = await recreate_tvh_service(
                    session,
                    arg,
                    mux_uuid,
                    private_pid=abertis_data_pid,
                    service_sid=service_sid,
                )

                await recreate_tvh_iptv_mux(
                    session,
                    arg,
                    iptv_network_uuid=abertis_net_uuid,
                    svc_mux_uuid=svc_mux_uuid,
                    private_pid=abertis_data_pid,
                )

                map_dataPID_SID[abertis_data_pid] = service_sid

    for p_pid, pid_ca in sorted(_MAP_PPID_CA.items()):
        logger.info(
            f"F {p_pid:04X}{pid_ca:04X} 00000000 FFFFFFFFFFFFFFFF ;ABERTIS-abertpy {p_pid} (30.0W)"
        )


@app.command(help="Install everything on TVHeadend")
def setup(arg: SetupArgs):
    return asyncio.run(setup_async(arg))
