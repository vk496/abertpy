import asyncio
import shutil
import subprocess
import sys
from datetime import timedelta
from pathlib import Path
from typing import Annotated, Self

import aiohttp
import pydantic
import typer
from loguru import logger
from pydantic import ByteSize, Field

from abertpy import _HARDCODED_KEY
from abertpy.helpers import (
    extract_ppid_from_svcname,
    tvh_find_abertpy_network,
    tvh_get_muxes,
    tvh_get_networks,
    tvh_set_mux_iptv_url,
)

_REFERENCE_PING = "ping"
_REFERENCE_PROXY = "proxy"


class CommonArgs(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(validate_default=True)

    debug: Annotated[
        bool,
        typer.Option(
            "-d",
            "--debug",
            help="Debug info",
        ),
    ] = False

    tvheadend_url: Annotated[
        pydantic.HttpUrl,
        typer.Option(
            "-t",
            "--tvhurl",
            help="Base URL of the TVheadend server. Ex: http://tvheadend.lan:9981/doesnt_matter_the_path",
            metavar="URL",
        ),
    ]

    @pydantic.field_validator("debug")
    @classmethod
    def set_debug(cls, debug):
        logger.remove()

        logger.add(sys.stderr, level="DEBUG" if debug else "INFO")

        return debug

    @pydantic.field_validator("tvheadend_url")
    @classmethod
    def validate_url(cls, tvheadend_url):
        async def validate_tvheadend_url(tvheadend_url):
            base_url = str(tvheadend_url).removesuffix(tvheadend_url.path or "/")
            serverinfo_url = base_url + "/api/mpegts/mux/grid"

            async with aiohttp.ClientSession() as session:
                async with session.get(serverinfo_url) as response:
                    if response.status != 200:
                        raise ValueError(
                            f"TVheadend error {response.status}. Check user credentials and access permissions."
                        )
            return tvheadend_url

        return asyncio.run(validate_tvheadend_url(tvheadend_url))

    def get_base_url(self) -> str:
        return str(self.tvheadend_url).removesuffix(self.tvheadend_url.path or "/")


class ProxyArgs(CommonArgs):
    model_config = pydantic.ConfigDict(validate_default=True)

    service_uuid: Annotated[
        str,
        typer.Option(
            "-s",
            "--service",
            help="UUID of the Abertis service to proxy",
        ),
    ]

    allowed_pid: Annotated[
        int,
        typer.Option(
            "-a",
            "--allowed-pids",
            help="Allowed MPEG TS PID for decapsulation",
        ),
    ]

    read_chunk_log2: Annotated[
        int,
        typer.Option(
            " ",
            "--read-chunk-log2",
            help=(
                "log2 of the read/write batch size in bytes (16 = 64KB). Reading "
                "and writing one 188-byte TS frame at a time (as this used to do) "
                "measured ~5x more CPU than batching against a real captured "
                "channel; 14-20 all perform well, with returns past that "
                "flattening and then reversing (past ~22) from larger buffer "
                "allocation/copy overhead outweighing the saved call overhead."
            ),
            metavar="N",
        ),
    ] = Field(default=16, ge=8, le=24)

    dvb_mux: Annotated[
        str,
        typer.Option(
            " ",
            "--dvb-mux",
            help=(
                "Name of the real transponder this pPID is expected to live on "
                "(e.g. 11302H), baked into the pipe command at setup time. The "
                "same pPID number legitimately repeats across different "
                "transponders, so this is what lets the self-heal below (for "
                "when TVheadend's own scan prunes the hijacked service) pick "
                "the right one instead of any same-numbered override anywhere "
                "in the system. Empty for pipe commands installed before this "
                "option existed -- those can only self-heal when the pPID "
                "happens to be unique network-wide."
            ),
        ),
    ] = ""

    @pydantic.model_validator(mode="after")
    def validate_service_uuid(self):
        async def fetch_svcs(base_url: str) -> list[dict]:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    base_url + "/api/mpegts/service/grid",
                    data={"hidemode": "none", "limit": 10000},
                ) as response:
                    if response.status != 200:
                        raise ValueError(
                            f"TVheadend error {response.status}. Check user credentials and access permissions."
                        )
                    resp = await response.json()
            return resp.get("entries", [])

        def find_candidates(svcs: list[dict]) -> list[dict]:
            # Only an enabled override can actually stream, and (when known)
            # only one living on the transponder this pipe command was built
            # for is a safe substitute: the same pPID number legitimately
            # repeats across transponders, so a bare pPID match elsewhere in
            # the system could silently swap in a different channel's
            # content -- this is exactly what happened before this uuid went
            # stale-and-ambiguous once.
            return [
                svc
                for svc in svcs
                if svc.get("enabled")
                and _HARDCODED_KEY in svc.get("svcname", "")
                and extract_ppid_from_svcname(svc.get("svcname", ""))
                == self.allowed_pid
                and (not self.dvb_mux or svc.get("multiplex", "") == self.dvb_mux)
            ]

        async def migrate_pipe_command(
            base_url: str, original_uuid: str, resolved_uuid: str, transponder: str
        ) -> None:
            # Reached only when this pipe command predates --dvb-mux, e.g. a
            # mux installed by an older abertpy version whose config TVheadend
            # keeps verbatim across an upgrade. Patch the mux's own stored
            # command in place (matched on the uuid as originally invoked
            # with -- still literally embedded in it, even once stale) so
            # every future play already carries the transponder hint and
            # never has to take this slow path again.
            if not transponder:
                return

            async with aiohttp.ClientSession() as session:
                muxes = (await tvh_get_muxes(session, base_url)).get("entries", [])
                mux = next(
                    (m for m in muxes if original_uuid in m.get("iptv_url", "")),
                    None,
                )
                if mux is None:
                    logger.debug(
                        "Could not find the mux this pipe command belongs to; "
                        "not migrating"
                    )
                    return

                new_url = mux["iptv_url"]
                if original_uuid != resolved_uuid:
                    new_url = new_url.replace(original_uuid, resolved_uuid)
                new_url = f"{new_url} --dvb-mux {transponder}"

                await tvh_set_mux_iptv_url(session, base_url, mux["uuid"], new_url)
                logger.info(
                    "Migrated {} to carry --dvb-mux {}",
                    mux.get("iptv_muxname", mux["uuid"]),
                    transponder,
                )

        async def resolve(tvheadend_url, service_uuid: str) -> str:
            base_url = str(tvheadend_url).removesuffix(tvheadend_url.path or "/")

            svcs = await fetch_svcs(base_url)
            resolved_uuid: str | None = None
            transponder = ""

            exact = next(
                (svc for svc in svcs if svc.get("uuid", "") == service_uuid), None
            )
            if exact is not None:
                resolved_uuid = service_uuid
                transponder = exact.get("multiplex", "")
            else:
                candidates = find_candidates(svcs)
                if len(candidates) == 1:
                    resolved_uuid = candidates[0]["uuid"]
                    transponder = candidates[0].get("multiplex", "")
                    logger.warning(
                        "Service UUID {} not found. Using service with "
                        "matching pPID {} on {} and UUID {}.",
                        service_uuid,
                        self.allowed_pid,
                        transponder,
                        resolved_uuid,
                    )
                elif not self.dvb_mux:
                    raise ValueError(
                        f"Service UUID {service_uuid} not found in TVheadend, "
                        f"and {len(candidates)} candidate(s) share pPID "
                        f"{self.allowed_pid} network-wide -- refusing to guess "
                        "which transponder without a --dvb-mux hint. Re-run "
                        "setup to regenerate this pipe command."
                    )
                else:
                    # The override is gone and, on this specific transponder,
                    # either missing entirely or still duplicated -- both are
                    # exactly what a targeted rescan of just this transponder
                    # fixes (it reaps duplicates and recreates a missing
                    # override), so trigger one instead of leaving the
                    # channel dead until someone notices.
                    logger.warning(
                        "Service UUID {} not found and {} candidate(s) for "
                        "pPID {} on {}; triggering a targeted rescan of {}",
                        service_uuid,
                        len(candidates),
                        self.allowed_pid,
                        self.dvb_mux,
                        self.dvb_mux,
                    )

                    async with aiohttp.ClientSession() as session:
                        network_uuid = await tvh_find_abertpy_network(
                            session, base_url
                        )

                    if network_uuid:
                        ret = subprocess.run(
                            [
                                sys.argv[0],
                                "setup",
                                "--mux",
                                self.dvb_mux,
                                "--network-uuid",
                                network_uuid,
                                "-t",
                                base_url,
                                "--no--validate-abertpy",
                            ],
                            capture_output=True,
                            text=True,
                        )
                        if ret.returncode != 0:
                            logger.warning(
                                "Rescan of {} exited {}: {}",
                                self.dvb_mux,
                                ret.returncode,
                                ret.stderr.strip(),
                            )
                        svcs = await fetch_svcs(base_url)
                        candidates = find_candidates(svcs)
                    else:
                        logger.warning(
                            "Could not find the abertpy IPTV network; "
                            "skipping rescan"
                        )

                    if len(candidates) == 1:
                        resolved_uuid = candidates[0]["uuid"]
                        transponder = candidates[0].get("multiplex", "") or self.dvb_mux
                        logger.warning(
                            "Rescan of {} resolved pPID {} to UUID {}",
                            self.dvb_mux,
                            self.allowed_pid,
                            resolved_uuid,
                        )
                    else:
                        raise ValueError(
                            f"Service UUID {service_uuid} not found in "
                            f"TVheadend, and a rescan of {self.dvb_mux} left "
                            f"{len(candidates)} candidate(s) for pPID "
                            f"{self.allowed_pid} instead of exactly one."
                        )

            if not self.dvb_mux:
                await migrate_pipe_command(
                    base_url, service_uuid, resolved_uuid, transponder
                )

            return resolved_uuid

        self.service_uuid = asyncio.run(
            resolve(self.tvheadend_url, self.service_uuid)
        )
        return self


class CleanupArgs(CommonArgs):
    model_config = pydantic.ConfigDict(validate_default=True)

    apply: Annotated[
        bool,
        typer.Option(
            " ",
            "--apply/--dry-run",
            help="Actually delete the stale services. Default only reports them.",
        ),
    ] = False


class SetupArgs(CommonArgs):
    model_config = pydantic.ConfigDict(validate_default=True)

    network_uuid: Annotated[
        str | None,
        typer.Option(
            "-n",
            "--network-uuid",
            help="DVB-S Network containing Abertis muxes (usually Hispasat 30W). Empty to list all networks.",
        ),
    ] = None

    tsanalyze_path: Annotated[
        Path | None,
        typer.Option(
            " ",
            "--path-tsanalyze",
            help="Path to the tsanalyze binary from TSDuck. Will search for 'tsanalyze' by default",
            exists=True,
            file_okay=True,
            dir_okay=False,
            writable=False,
            readable=True,
            resolve_path=True,
        ),
    ] = None

    mux_buffer_size: Annotated[
        ByteSize,
        typer.Option(
            " ",
            "--max-buffer-size",
            help="Amount of data to buffer from each mux before analyzing",
            metavar="SIZE",
        ),
    ] = Field(
        default="50MB"
    )  # type: ignore

    mux_buffer_time: Annotated[
        timedelta,
        typer.Option(
            " ",
            "--max-buffer-time",
            help="Maximum time to wait for buffering each mux before analyzing",
            metavar="ISO 8601",
        ),
    ] = "PT10S"  # type: ignore

    abertpy_path: Annotated[
        Path | None,
        typer.Option(
            " ",
            "--path-abertpy",
            help="Command to execute for self referencing abertpyin IPTV mux. It will be injected later for TVHeadend IPTV proxy",
        ),
    ] = Path("/usr/local/bin/abertpy")

    abertpy_validate_binary: Annotated[
        bool,
        typer.Option(
            " ",
            "--validate-abertpy/--no--validate-abertpy",
            help="Validate the abertpy binary. Disable if running outside the TVHeadend host",
        ),
    ] = True

    fast_scan: Annotated[
        bool,
        typer.Option(
            " ",
            "--fast-scan/--no-fast-scan",
            help="Only scan the muxes known to carry Abertis instead of the whole network. Faster, but misses any Abertis mux not in the known list. Default scans everything.",
        ),
    ] = False

    only_muxes: Annotated[
        list[str],
        typer.Option(
            " ",
            "--mux",
            help="Scan only these mux names (repeatable), e.g. --mux 11675H --mux 12631V. Names not present as targets are ignored. Overrides --fast-scan. Default scans everything.",
            metavar="NAME",
        ),
    ] = []

    proxy_url: Annotated[
        pydantic.HttpUrl,
        typer.Option(
            " ",
            "--proxy-url",
            help="TVheadend URL baked into the installed proxy pipe command. Runs on the TVheadend host, so it usually points to localhost.",
            metavar="URL",
        ),
    ] = Field(
        default="http://127.0.0.1:9981/"
    )  # type: ignore

    iptv_pipe_string: Annotated[
        str,
        typer.Option(
            " ",
            "--pipe-command",
            help="""DVB-S Network containing Abertis muxes (usually Hispasat 30W). Empty to list all networks. Allowed variables:
            \n
            * abertpy_path: Full path to abertpy command\n
            * allowed_pid: Private PID number of the REMUX\n
            * svc_mux_uuid: UUID of the service REMUX\n
            * tvheadend_url: Path for tvheadend_url base URL\n
            * proxy_url: URL baked into the proxy command (--proxy-url)\n
            * dvb_mux_name: Name of the real transponder this pPID lives on (e.g. 11302H)
            """,
        ),
    ] = (
        "pipe://{abertpy_path} proxy -a {allowed_pid} -t {proxy_url} "
        "-s {svc_mux_uuid} --dvb-mux {dvb_mux_name}"
    )

    @pydantic.model_validator(mode="after")
    def validate_abertpy_path(self):
        if self.abertpy_validate_binary:

            try:
                full_cmd: list[str] = [str(self.abertpy_path)]
                full_cmd.append(_REFERENCE_PING)
                ret = subprocess.run(full_cmd, capture_output=True, text=True)
                if "vk496" not in ret.stdout or ret.returncode != 18:
                    raise ValueError(f"Bad abertpy reference:\n {ret}")

            except Exception as e:
                raise ValueError(
                    "Bad abertpy binary. Consider disabling this validation"
                ) from e

        return self

    @pydantic.field_validator("tsanalyze_path")
    @classmethod
    def validate_tsduck(cls, tsanalyze_path):
        bin_path: str | None = (
            str(tsanalyze_path) if tsanalyze_path else shutil.which("tsanalyze")
        )

        if not bin_path:
            raise ValueError(
                "tsanalyze binary not found. Please install TSDuck (https://tsduck.io/) or provide the argument path to tsanalyze"
            )

        try:
            ret = subprocess.run(
                [bin_path, "--version"], capture_output=True, text=True
            )
            if "TSDuck" not in ret.stderr:
                raise ValueError(f"Wrong tsanalyze binary?. {ret}")
        except Exception as e:
            raise ValueError("Error running tsanalyze") from e

        return Path(bin_path)

    @pydantic.model_validator(mode="after")
    def validate_network_uuid(self) -> Self:
        async def validate_network():
            base_url = self.get_base_url()

            async with aiohttp.ClientSession() as session:
                networks = await tvh_get_networks(session, base_url)

                return {
                    network["uuid"]: network["networkname"]
                    for network in networks.get("entries", [])
                }

        all_networks: dict[str, str] = asyncio.run(validate_network())

        if self.network_uuid in all_networks:
            return self

        # No (valid) network selected: print the available ones cleanly and exit
        # instead of surfacing a noisy validation traceback.
        if self.network_uuid is None:
            typer.echo("Select a network with --network-uuid. Available networks:")
        else:
            typer.echo(
                f"Network UUID {self.network_uuid!r} not found. Available networks:",
                err=True,
            )

        width = max((len(u) for u in all_networks), default=0)
        for net_uuid, net_name in all_networks.items():
            typer.echo(f"  {net_uuid:<{width}}  {net_name}")

        raise typer.Exit(0 if self.network_uuid is None else 1)

    def get_iptv_pipe(
        self, svc_mux_uuid: str, allowed_pid: int, dvb_mux_name: str
    ) -> str:
        return self.iptv_pipe_string.format(
            abertpy_path=self.abertpy_path,
            tvheadend_url=self.tvheadend_url,
            proxy_url=self.proxy_url,
            svc_mux_uuid=svc_mux_uuid,
            allowed_pid=allowed_pid,
            dvb_mux_name=dvb_mux_name,
        )
