import sys
from venv import logger

import backoff
import requests
from pydantic_typer import Typer

from abertpy.models import ProxyArgs

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


@backoff.on_exception(backoff.expo, ConnectionError, max_time=10)
@app.command(help="Proxy for Abertis streams")
def proxy(arg: ProxyArgs):

    base_url = arg.get_base_url()
    endpoint = f"{base_url}/stream/service/{arg.service_uuid}"

    response = requests.get(
        endpoint, stream=True, headers={"User-Agent": "curl/aiohttp"}
    )
    for packet in response.iter_content(chunk_size=FRAME_SIZE):
        process_data(packet=packet, allowed_pid=arg.allowed_pid)
