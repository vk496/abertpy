import asyncio
import sys

import aiohttp
import backoff
from asyncstdlib import batched, chain
from pydantic_typer import Typer

from abertpy.models import ProxyArgs

app = Typer()

######################################
######################################
######################################
######################################


FRAME_SIZE = 188
FAME_BUFFER = 100

AFC_PAYLOAD_ONLY = 0x10
AFC_ADAPTATION_PAYLOAD = 0x30


@backoff.on_exception(backoff.expo, aiohttp.ServerDisconnectedError, max_tries=8, max_time=60)
async def proxy_async(arg: ProxyArgs):
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
                chain.from_iterable(response.content.iter_chunked(FRAME_SIZE * FAME_BUFFER)),
                FRAME_SIZE,
            )

            async for packet in chunks:
                try:
                    afc = packet[3] & 0x30
                except IndexError:
                    continue

                payload: bytes | None = None

                if afc == AFC_PAYLOAD_ONLY:
                    payload = bytes(packet[4:])
                elif afc == AFC_ADAPTATION_PAYLOAD:
                    payload = bytes(packet[packet[4] + 5 :])

                if payload:
                    sys.stdout.buffer.write(payload)
                    # sys.stdout.flush()


@app.command(help="Proxy for Abertis streams")
def proxy(arg: ProxyArgs):
    return asyncio.run(proxy_async(arg))
