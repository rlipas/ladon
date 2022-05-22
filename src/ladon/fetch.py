import asyncio
import bz2
import logging
from importlib import import_module

import ujson

from ladon.database import SqliteDatabase

logger = logging.getLogger(__name__)


async def fetch(provider_name, interval="1d", symbols=None):
    provider = import_module(f"ladon.providers.{provider_name}")

    filename = (
        f"{provider_name}_{interval}{'_'+'_'.join(symbols) if symbols else ''}.json.bz2"
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

    db = SqliteDatabase()

    info = await provider.exchange_info()
    db.set_provider_info(provider_name, info)

    if symbols is not None:
        logger.info(f"Filtering symbols {symbols}")
        info["symbols"] = list(
            filter(lambda s: s["symbol"] in symbols, info["symbols"])
        )

    async def fetch_until_end(symbol):
        symbol["klines"] = []

        if symbol["pair"] in old_klines:
            symbol["klines"] += old_klines[symbol["pair"]][:-1]
            start_time = old_klines[symbol["pair"]][-1][0] // 1000
        else:
            start_time = symbol["onboardDate"] // 1000

        logger.info(f"Fetching {symbol['pair']} {symbol['contractType']}...")
        symbol["klines"] += await provider.continuousKlines(
            symbol["pair"],
            symbol["contractType"],
            interval=interval,
            start_time=start_time,
            limit=1500,
            fetch_all=True,
        )

        db.add_candlesticks(
            provider_name,
            symbol["pair"],
            interval,
            {k[0] // 1000: k for k in symbol["klines"]},
        )

        if len(symbol["klines"]) == 0:
            del symbol["klines"]

        return True

    await asyncio.gather(*map(fetch_until_end, info["symbols"]))

    with bz2.open(filename, mode="w") as f:
        logger.info(f"Writing {filename}...")
        f.write(ujson.dumps(info).encode())

    await provider.close()
