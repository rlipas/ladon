import asyncio
import logging
from importlib import import_module

from ladon.database import SqliteDatabase

logger = logging.getLogger(__name__)


async def fetch(provider_name, interval="1d", symbols=None):
    provider = import_module(f"ladon.providers.{provider_name}")

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

        last_kline = db.get_candlestick(provider_name, symbol["symbol"], interval)
        if last_kline is not None:
            start_time = last_kline[0] // 1000
        elif "onboardDate" in symbol:
            start_time = symbol["onboardDate"] // 1000
        else:
            start_time = 1483228800  # 2017

        logger.info(
            f"Fetching {symbol['symbol']}, starting@{start_time}..."
        )
        new_klines = await provider.continuousKlines(
            symbol["symbol"],
            None, #symbol["contractType"],
            interval=interval,
            start_time=start_time,
            limit=1000,
            fetch_all=True,
        )

        db.add_candlesticks(
            provider_name,
            symbol["symbol"],
            interval,
            {k[0] // 1000: (k[1], k[2], k[3], k[4], k[5], k[7], k) for k in new_klines},
        )

        return True

    await asyncio.gather(*map(fetch_until_end, info["symbols"]))

    await provider.close()
