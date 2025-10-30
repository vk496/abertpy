import asyncio
import sys
from venv import logger

import aiohttp
import backoff
from asyncstdlib.itertools import batched, chain
from pydantic_typer import Typer

from abertpy.models import ProxyArgs

app = Typer()

######################################
######################################
######################################
######################################


FRAME_SIZE = 188
FAME_BUFFER = 20
MPEG_TS_START_BYTE = 0x47

AFC_PAYLOAD_ONLY = 0x10
AFC_ADAPTATION_PAYLOAD = 0x30


@backoff.on_exception(
    backoff.expo, aiohttp.ServerDisconnectedError, max_tries=8, max_time=60
)
async def reader(arg: ProxyArgs):
    base_url = arg.get_base_url()

    endpoint = f"{base_url}/stream/service/{arg.service_uuid}"

    failed_start_bytes = 0

    async with aiohttp.ClientSession(
        raise_for_status=True,
        headers={
            "User-Agent": "curl/aiohttp"
        },  # https://docs.tvheadend.org/documentation/development/json-api/other-functions#play
    ) as session:
        async with session.get(endpoint) as response:
            chunks = batched(
                chain.from_iterable(
                    response.content.iter_chunked(FRAME_SIZE * FAME_BUFFER)
                ),
                FRAME_SIZE,
            )

            async for packet in chunks:
                if not packet or packet[0] != MPEG_TS_START_BYTE:
                    if failed_start_bytes > 100:
                        raise ConnectionError("MPEG-TS stream de-synced")

                    failed_start_bytes += 1
                    logger.error("MPEG-TS first byte unexpected")
                    continue

                try:
                    afc = packet[3] & 0x30
                except IndexError as e:
                    logger.error(e)
                    continue

                payload: bytes | None = None

                if afc == AFC_PAYLOAD_ONLY:
                    payload = bytes(packet[4:])
                elif afc == AFC_ADAPTATION_PAYLOAD:
                    payload = bytes(
                        packet[packet[4] + 5 :]
                    )  # 4 bytes header + 1 byte length indicator
                else:
                    logger.error("AFC bad value {}", afc)
                    continue

                if payload:
                    sys.stdout.buffer.write(payload)
                    sys.stdout.flush()


@app.command(help="Proxy for Abertis streams")
def proxy(arg: ProxyArgs):
    return asyncio.run(reader(arg))
