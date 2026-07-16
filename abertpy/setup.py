import asyncio
import json
import subprocess

import aiohttp
from loguru import logger
from pydantic_typer import Typer

from abertpy import _HARDCODED_KEY
from abertpy.helpers import (
    patch_original_SID_svc,
    tvh_delete_svcs,
    tvh_find_overrides,
    tvh_get_muxes,
    tvh_get_networks,
    tvh_get_svc_SID,
    tvh_set_mux_iptv_url,
    tvh_svc_mux_name,
)
from abertpy.models import SetupArgs

app = Typer()

_MAP_PPID_CA: dict[int, int] = {}


# Default Abertis (Cellnex) transponders on Hispasat 30.0W, installed on the DVB-S
# network when missing so the user has the Abertis muxes to scan out of the box.
# Only Abertis-operated transponders are listed here (not the whole satellite).
# Snapshot of the muxes confirmed working (scanned OK, carrying abertpy services)
# on a live TVheadend. khz is the frequency in kHz, ksym the symbol rate in kSym/s.
# One transponder per line — the fmt: off/on below keeps the formatter from wrapping.
# fmt: off
DEFAULT_ABERTIS_MUXES: list[dict] = [
    {"khz": 11222000, "pol": "H", "ksym": 30000, "fec": "3/4", "mod": "PSK/8", "delsys": "DVB-S2", "rolloff": "35", "pilot": "AUTO"},
    {"khz": 11302000, "pol": "H", "ksym": 30000, "fec": "3/4", "mod": "PSK/8", "delsys": "DVB-S2", "rolloff": "35", "pilot": "AUTO"},
    {"khz": 11347000, "pol": "H", "ksym": 20858, "fec": "3/4", "mod": "PSK/8", "delsys": "DVB-S2", "rolloff": "35", "pilot": "AUTO"},
    {"khz": 11382000, "pol": "H", "ksym": 30000, "fec": "3/4", "mod": "PSK/8", "delsys": "DVB-S2", "rolloff": "35", "pilot": "AUTO"},
    {"khz": 11502000, "pol": "V", "ksym": 10200, "fec": "3/4", "mod": "PSK/8", "delsys": "DVB-S2", "rolloff": "35", "pilot": "AUTO"},
    {"khz": 11653000, "pol": "H", "ksym": 19680, "fec": "3/4", "mod": "PSK/8", "delsys": "DVB-S2", "rolloff": "35", "pilot": "AUTO"},
    {"khz": 11675000, "pol": "H", "ksym": 15750, "fec": "3/4", "mod": "PSK/8", "delsys": "DVB-S2", "rolloff": "35", "pilot": "AUTO"},
    {"khz": 12548000, "pol": "V", "ksym": 29600, "fec": "3/4", "mod": "PSK/8", "delsys": "DVB-S2", "rolloff": "35", "pilot": "AUTO"},
    {"khz": 12584000, "pol": "V", "ksym": 16900, "fec": "3/4", "mod": "PSK/8", "delsys": "DVB-S2", "rolloff": "35", "pilot": "AUTO"},
    {"khz": 12631000, "pol": "V", "ksym": 30000, "fec": "3/4", "mod": "PSK/8", "delsys": "DVB-S2", "rolloff": "35", "pilot": "AUTO"},
    {"khz": 12671000, "pol": "V", "ksym": 30000, "fec": "3/4", "mod": "PSK/8", "delsys": "DVB-S2", "rolloff": "35", "pilot": "AUTO"},
]
# fmt: on

# A transponder is considered already installed if an existing mux shares its
# polarisation and sits within this many kHz (absorbs small freq reporting diffs;
# Abertis transponders are tens of MHz apart, so this never conflates two of them).
_FREQ_MATCH_KHZ = 2500

# Muxes (by TVheadend name) known to carry Abertis. Only these are scanned in
# fast-scan mode; a full scan (the default) looks at the whole network.
ABERTIS_SCAN_MUXES: frozenset[str] = frozenset(
    {
        "11222H",
        "11302H",
        "11347H",
        "11382H",
        "11502V",
        "11653H",
        "11675H",
        "12548V",
        "12584V",
        "12631V",
        "12671V",
    }
)


async def create_default_abertis_muxes(
    session: aiohttp.ClientSession, arg: SetupArgs
) -> None:
    """Install the default Abertis transponders into the DVB-S network if missing."""
    existing = await tvh_get_muxes(session, arg.get_base_url())

    # TVheadend stores DVB-S frequency in kHz and symbol rate in Sym/s
    existing_muxes: list[tuple[int, str]] = [
        (mux.get("frequency", 0), mux.get("polarisation", ""))
        for mux in existing.get("entries", [])
        if mux.get("network_uuid", None) == arg.network_uuid
    ]

    created = 0
    for mux in DEFAULT_ABERTIS_MUXES:
        if any(
            pol == mux["pol"] and abs(freq - mux["khz"]) <= _FREQ_MATCH_KHZ
            for freq, pol in existing_muxes
        ):
            continue

        async with session.post(
            arg.get_base_url() + "/api/mpegts/network/mux_create",
            data={
                "uuid": arg.network_uuid,
                "conf": json.dumps(
                    {
                        "enabled": 1,
                        "delsys": mux["delsys"],
                        "frequency": mux["khz"],
                        "symbolrate": mux["ksym"] * 1000,
                        "polarisation": mux["pol"],
                        "modulation": mux["mod"],
                        "fec": mux["fec"],
                        "rolloff": mux["rolloff"],
                        "pilot": mux["pilot"],
                    }
                ),
            },
        ):
            pass

        created += 1
        logger.debug(f"Created Abertis mux {mux['khz'] // 1000}{mux['pol']}")

    if created:
        logger.info(f"Installed {created} missing default Abertis mux(es)")
    else:
        logger.info("All default Abertis muxes already present")


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


def select_muxes_to_scan(arg: SetupArgs, muxes: list[dict]) -> list[dict]:
    """Restrict which muxes to scan: --mux names, else --fast-scan, else all."""
    if arg.only_muxes:
        wanted = set(arg.only_muxes)
        present = {mux.get("name", "") for mux in muxes}
        for name in sorted(wanted - present):
            logger.warning(
                "Requested mux {} is not a target in this network, ignoring", name
            )
        selected = [mux for mux in muxes if mux.get("name", "") in wanted]
        logger.info("Scanning {} requested mux(es)", len(selected))
        return selected

    if arg.fast_scan:
        selected = [mux for mux in muxes if mux.get("name", "") in ABERTIS_SCAN_MUXES]
        logger.info("Fast scan: {} known Abertis mux(es)", len(selected))
        return selected

    return muxes


async def wait_dvbs_tuners_idle(
    session: aiohttp.ClientSession, arg: SetupArgs, timeout: float = 20.0
) -> None:
    """Block until no DVB-S tuner has an active subscription.

    Each mux scan subscribes to a transponder; if the next mux is streamed
    before TVheadend releases the previous subscription and retunes, the new
    stream delivers the still-tuned transponder's data (pids leaking between
    muxes). Waiting for the satellite tuners to go idle serialises this.
    """
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while True:
        async with session.get(arg.get_base_url() + "/api/status/inputs") as response:
            data = await response.json()

        busy = any(
            "DVB-S" in entry.get("input", "") and entry.get("subs", 0)
            for entry in data.get("entries", [])
        )
        if not busy or loop.time() >= deadline:
            if busy:
                logger.warning("DVB-S tuners still busy after {}s", timeout)
            return

        await asyncio.sleep(0.5)


async def fetch_mux_data(
    session: aiohttp.ClientSession,
    url: str,
    arg: SetupArgs,
    buffer: bytearray,
):
    async with session.get(url) as response:
        try:
            total_bytes = 0
            async for data in response.content.iter_chunked(1024 * 10):
                total_bytes += len(data)
                buffer.extend(data)
                if total_bytes >= arg.mux_buffer_size:
                    logger.debug(f"Stopped after receiving {total_bytes} bytes.")
                    return
        finally:
            # Force-close so TVheadend drops the subscription and frees the tuner
            # promptly, instead of lingering on keep-alive.
            response.close()


async def get_mux_data(
    session: aiohttp.ClientSession,
    arg: SetupArgs,
    url: str,
):
    # Make sure the previous mux fully released its tuner before we subscribe,
    # otherwise this scan can read the previously-tuned transponder's stream.
    await wait_dvbs_tuners_idle(session, arg)

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


# The satellite tuner intermittently locks onto the wrong transponder (or fails
# to lock), so a scan can return another mux's stream. Retry a bounded number of
# times, verifying the analysed TS id against the mux's known tsid, so we never
# build overrides from the wrong mux and never loop forever.
_MAX_SCAN_ATTEMPTS = 3


async def scan_mux_verified(
    session: aiohttp.ClientSession, arg: SetupArgs, mux: dict
) -> dict | None:
    """Scan a mux, returning its tsanalyze output only if it tuned correctly.

    Returns None if, after _MAX_SCAN_ATTEMPTS, the tuner never locked onto the
    requested transponder (verified via TS id) so the caller can skip it.
    """
    mux_name = mux.get("name", "")
    expected_tsid = mux.get("tsid")
    url = f"{arg.get_base_url()}/play/ticket/stream/mux/{mux['uuid']}"

    for attempt in range(1, _MAX_SCAN_ATTEMPTS + 1):
        tsanalyzer_dict = await get_mux_data(session, arg, url)
        actual_tsid = tsanalyzer_dict.get("ts", {}).get("id")

        if not actual_tsid:
            logger.warning(
                "MUX {} scan {}/{}: no transport stream locked, retrying",
                mux_name,
                attempt,
                _MAX_SCAN_ATTEMPTS,
            )
        elif not expected_tsid or actual_tsid == expected_tsid:
            # Correct transponder (or nothing to verify against) -> accept
            return tsanalyzer_dict
        else:
            logger.warning(
                "MUX {} scan {}/{}: tuned to wrong transponder "
                "(got tsid {}, expected {}), retrying",
                mux_name,
                attempt,
                _MAX_SCAN_ATTEMPTS,
                actual_tsid,
                expected_tsid,
            )

        # Give the tuner a chance to retune cleanly before the next attempt
        await asyncio.sleep(2)

    logger.error("MUX {} could not be scanned reliably, skipping", mux_name)
    return None


async def create_iptv_network(session: aiohttp.ClientSession, arg: SetupArgs) -> str:

    # Find network with pnetworkname containing abertpy
    networks = await tvh_get_networks(session, arg.get_base_url())

    for network in networks.get("entries", []):
        if _HARDCODED_KEY in network.get("networkname", ""):
            logger.info(f"IPTV Network already exist: {network.get('networkname')}")
            return network.get("uuid")

    async with (
        session.post(
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
        ) as response
    ):
        net_uuid: dict[str, str] = await response.json()
        return net_uuid["uuid"]


async def tvh_find_service_uuid(
    session: aiohttp.ClientSession,
    arg: SetupArgs,
    mux_uuid: str,
    service_sid: int,
) -> str:
    service = await tvh_get_svc_SID(
        session, arg.get_base_url(), service_sid, mux_uuid=mux_uuid
    )
    if service is None:
        raise ValueError(
            f"Service UUID not found for SID {service_sid} in mux {mux_uuid}"
        )

    return service["uuid"]


async def recreate_tvh_service(
    session: aiohttp.ClientSession,
    arg: SetupArgs,
    mux_uuid: str,
    private_pid: int,
    service_sid: int,
) -> str:

    overrides = await tvh_find_overrides(
        session, arg.get_base_url(), mux_uuid, private_pid
    )

    # Reuse an existing override, else hijack the service TVheadend scanned. Both
    # go through raw/export, which preserves the uuid on re-import, so the node we
    # read here is the node the mux ends up streaming from.
    svc_uuid: str = (
        overrides[0]["uuid"]
        if overrides
        else await tvh_find_service_uuid(session, arg, mux_uuid, service_sid)
    )

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
    if overrides:
        stale = [svc["uuid"] for svc in overrides[1:]]
        if stale:
            deleted = await tvh_delete_svcs(session, arg.get_base_url(), stale)
            logger.info("pPID {}: reaped {} stale override(s)", private_pid, deleted)
        return svc_uuid

    patch_original_SID_svc(sid_original, private_pid, str(service_sid))

    async with session.post(
        arg.get_base_url() + "/api/raw/import",
        data={
            "node": json.dumps(sid_original),
        },
    ) as response:
        pass

    return svc_uuid


async def recreate_tvh_iptv_mux(
    session: aiohttp.ClientSession,
    arg: SetupArgs,
    iptv_network_uuid: str,
    svc_mux_uuid: str,
    private_pid: int,
    mux_freq: str,
):

    target_muxname = f"{_HARDCODED_KEY}: MUX {mux_freq} pPID {private_pid}"
    iptv_url = arg.get_iptv_pipe(svc_mux_uuid=svc_mux_uuid, allowed_pid=private_pid)

    muxes = await tvh_get_muxes(session, arg.get_base_url())

    # Match the exact mux name. A loose substring check (e.g. "303" in the name)
    # would treat pPID 303 as already present when an unrelated mux exists for
    # pPID 2303 (or the same pPID on another transponder), skipping creation.
    for mux in muxes.get("entries", []):
        if (
            mux.get("network_uuid", None) == iptv_network_uuid
            and mux.get("iptv_muxname", "") == target_muxname
        ):
            # The mux outlives the service it names, so an existing one can still
            # point at a service that has since been replaced or reaped, leaving
            # it streaming from a disabled or dangling uuid.
            if mux.get("iptv_url", "") != iptv_url:
                await tvh_set_mux_iptv_url(
                    session, arg.get_base_url(), mux["uuid"], iptv_url
                )
                logger.info(
                    "pPID {}: repointed mux to service {}", private_pid, svc_mux_uuid
                )
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
                    "iptv_url": iptv_url,
                    "use_libav": 0,
                    "iptv_atsc": False,
                    "iptv_muxname": target_muxname,
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
    failed_muxes: list[str] = []

    async with (
        aiohttp.ClientSession(
            raise_for_status=True,
            headers={
                "User-Agent": "curl/aiohttp"
            },  # https://docs.tvheadend.org/documentation/development/json-api/other-functions#play
        ) as session
    ):
        # First thing, create a IPTV Network if not existing
        abertis_net_uuid = await create_iptv_network(session, arg)

        # Ensure the default Abertis transponders exist to scan against
        await create_default_abertis_muxes(session, arg)

        # Get enabled muxes from tvheadend, restricted by --mux / --fast-scan
        list_muxes = await get_muxes(session, arg)
        list_muxes = select_muxes_to_scan(arg, list_muxes)

        map_dataPID_SID: dict[int, int] = {}

        for mux in list_muxes:
            mux_uuid = mux["uuid"]
            mux_freq: str = mux.get("name", "")

            logger.debug(f"Scanning mux: {mux_uuid} - {mux_freq}")
            tsanalyzer_dict = await scan_mux_verified(session, arg, mux)
            if tsanalyzer_dict is None:
                # Tuner never locked onto this mux reliably; skip it rather than
                # create overrides from another transponder's stream.
                failed_muxes.append(mux_freq)
                continue

            found_p_pid = []

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

                found_p_pid.append(abertis_data_pid)

                svc_mux_uuid = await recreate_tvh_service(
                    session,
                    arg,
                    mux_uuid,
                    private_pid=abertis_data_pid,
                    service_sid=service_sid,
                )

                # Name the mux after the transponder the service actually sits
                # on, never the one we meant to tune. The two only diverge when
                # something went wrong -- a mis-locked tuner, or an override that
                # has since moved to where its SID really lives -- and taking the
                # intended name would bake that mistake into the label forever,
                # leaving a mux that streams one transponder while claiming
                # another. proxy resolves the same name from the service too, so
                # both agree on where a mux belongs.
                svc_mux_freq = (
                    await tvh_svc_mux_name(
                        session, arg.get_base_url(), svc_mux_uuid, abertis_data_pid
                    )
                    or mux_freq
                )
                if svc_mux_freq != mux_freq:
                    logger.warning(
                        "pPID {} was scanned on {} but its service lives on {}; "
                        "naming the mux after {}",
                        abertis_data_pid,
                        mux_freq,
                        svc_mux_freq,
                        svc_mux_freq,
                    )

                await recreate_tvh_iptv_mux(
                    session,
                    arg,
                    iptv_network_uuid=abertis_net_uuid,
                    svc_mux_uuid=svc_mux_uuid,
                    private_pid=abertis_data_pid,
                    mux_freq=svc_mux_freq,
                )

                map_dataPID_SID[abertis_data_pid] = service_sid

            logger.info(
                "MUX {} private pids: {}",
                mux_freq,
                ",".join(str(_) for _ in sorted(found_p_pid)),
            )

    for p_pid, pid_ca in sorted(_MAP_PPID_CA.items()):
        logger.info(
            f"F {p_pid:04X}{pid_ca:04X} 00000000 FFFFFFFFFFFFFFFF ;ABERTIS-abertpy {p_pid} (30.0W)"
        )

    if failed_muxes:
        retry = " ".join(f"--mux {name}" for name in failed_muxes)
        logger.warning(
            "{} mux(es) could not be scanned reliably: {}. Retry only these with: {}",
            len(failed_muxes),
            ", ".join(failed_muxes),
            retry,
        )


@app.command(help="Install everything on TVHeadend")
def setup(arg: SetupArgs):
    logger.info("Setup arguments:\n{}", arg.model_dump_json(indent=2))

    return asyncio.run(setup_async(arg))
