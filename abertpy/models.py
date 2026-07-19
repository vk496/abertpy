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
from abertpy.helpers import extract_ppid_from_svcname, tvh_get_networks

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

    @pydantic.model_validator(mode="after")
    def validate_service_uuid(self):
        async def validate_tvheadend_url(tvheadend_url, service_uuid):
            base_url = str(tvheadend_url).removesuffix(tvheadend_url.path or "/")
            serverinfo_url = base_url + "/api/mpegts/service/grid"

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    serverinfo_url,
                    data={"hidemode": "none", "limit": 10000},
                ) as response:
                    if response.status != 200:
                        raise ValueError(
                            f"TVheadend error {response.status}. Check user credentials and access permissions."
                        )
                    resp = await response.json()

            svcs: list = resp.get("entries", [])

            recreated_uuid: str | None = None

            for service in svcs:
                svc_uuid = service.get("uuid", "")
                if svc_uuid == service_uuid:
                    return service_uuid

                svcname: str = service.get("svcname", "")
                ppid = extract_ppid_from_svcname(svcname)
                if _HARDCODED_KEY in svcname and ppid and ppid == self.allowed_pid:
                    recreated_uuid = svc_uuid

            if recreated_uuid:
                logger.warning(
                    f"Service UUID {service_uuid} not found. Using service with matching pPID {self.allowed_pid} and UUID {recreated_uuid}."
                )
                return recreated_uuid

            raise ValueError(
                f"Service UUID {service_uuid} not found in TVheadend ({len(svcs)} services scanned)."
            )

        self.service_uuid = asyncio.run(
            validate_tvheadend_url(self.tvheadend_url, self.service_uuid)
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
            * proxy_url: URL baked into the proxy command (--proxy-url)
            """,
        ),
    ] = "pipe://{abertpy_path} proxy -a {allowed_pid} -t {proxy_url} -s {svc_mux_uuid}"

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

    def get_iptv_pipe(self, svc_mux_uuid: str, allowed_pid: int) -> str:
        return self.iptv_pipe_string.format(
            abertpy_path=self.abertpy_path,
            tvheadend_url=self.tvheadend_url,
            proxy_url=self.proxy_url,
            svc_mux_uuid=svc_mux_uuid,
            allowed_pid=allowed_pid,
        )
