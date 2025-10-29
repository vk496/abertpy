import aiohttp


async def tvh_get_networks(session: aiohttp.ClientSession, base_url: str):
    networks_url = base_url + "/api/mpegts/network/grid"
    async with session.get(networks_url) as response:
        networks: dict = await response.json()
        return networks


async def tvh_get_muxes(session: aiohttp.ClientSession, base_url: str):
    networks_url = base_url + "/api/mpegts/mux/grid"
    async with session.get(networks_url) as response:
        muxes: dict = await response.json()
        return muxes
