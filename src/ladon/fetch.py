import asyncio
import bz2

import ujson

from .providers.binance_futures import continuousKlines, exchange_info


async def fetch(interval, symbols):
    filename = (
        f"binance_futures_{interval}{'_'+'_'.join(symbols) if symbols else ''}.json.bz2"
    )

    old_klines = {}
    try:
        with bz2.open(filename, mode="r") as f:
            old_info = ujson.load(f)
            for symbol in old_info["symbols"]:
                if "klines" in symbol and len(symbol["klines"]) > 0:
                    old_klines[symbol["pair"]] = symbol["klines"]
    except FileNotFoundError:
        pass

    info = await exchange_info()

    async def fetch_until_end(symbol):
        if len(symbols) > 0 and symbol["symbol"] not in symbols:
            return False

        symbol["klines"] = []

        if symbol["pair"] in old_klines:
            symbol["klines"] += old_klines[symbol["pair"]][:-1]
            start_time = old_klines[symbol["pair"]][-1][0] // 1000
        else:
            start_time = symbol["onboardDate"] // 1000

        symbol["klines"] += await continuousKlines(
            symbol["pair"],
            symbol["contractType"],
            interval=interval,
            start_time=start_time,
            limit=1500,
            fetch_all=True,
        )

        if len(symbol["klines"]) == 0:
            del symbol["klines"]

        return True

    await asyncio.gather(*map(fetch_until_end, info["symbols"]))

    with bz2.open(filename, mode="w") as f:
        f.write(ujson.dumps(info).encode())
