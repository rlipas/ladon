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

        last_kline = db.get_candlestick(provider_name, symbol["pair"], interval)
        if last_kline is not None:
            start_time = last_kline[0] // 1000
        else:
            start_time = symbol["onboardDate"] // 1000

        logger.info(
            f"Fetching {symbol['pair']} {symbol['contractType']}, starting@{start_time}..."
        )
        new_klines = await provider.continuousKlines(
            symbol["pair"],
            symbol["contractType"],
            interval=interval,
            start_time=start_time,
            limit=1500,
            fetch_all=True,
        )

        db.add_candlesticks(
            provider_name,
            symbol["symbol"],
            interval,
            {k[0] // 1000: k for k in new_klines},
        )

        return True

    await asyncio.gather(*map(fetch_until_end, info["symbols"]))

    await provider.close()
