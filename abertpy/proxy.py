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

FAILED_START_BYTES = 0


def process_data(packet: bytes):
    global FAILED_START_BYTES

    if not packet or packet[0] != MPEG_TS_START_BYTE:
        if FAILED_START_BYTES > 100:
            raise ConnectionError("MPEG-TS stream de-synced")

        FAILED_START_BYTES += 1
        logger.error("MPEG-TS first byte unexpected")
        return

    try:
        afc = packet[3] & 0x30
    except IndexError as e:
        logger.error(e)
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


async def reader_pipe():
    packet = b"go go go"

    while packet:
        packet = sys.stdin.buffer.read(188)
        process_data(packet=packet)


@backoff.on_exception(
    backoff.expo, aiohttp.ServerDisconnectedError, max_tries=8, max_time=60
)
async def reader_url(arg: ProxyArgs):
    base_url = arg.get_base_url()

    endpoint = f"{base_url}/stream/service/{arg.service_uuid}"

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
                process_data(packet=bytes(packet))


@app.command(help="Proxy for Abertis streams")
def proxy(arg: ProxyArgs):
    if arg.pipe_input:
        return reader_pipe()

    return asyncio.run(reader_url(arg))
