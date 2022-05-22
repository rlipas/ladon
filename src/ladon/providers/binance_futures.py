import asyncio
import logging
from datetime import datetime

import httpx

from ladon.base import INTERVALS_MAP

logger = logging.getLogger(__name__)

BASE_URL = "https://fapi.binance.com"


class RateLimitedAsyncClient(httpx.AsyncClient):
    RATE_LIMIT_THRESHOLD = 0.9
    PERIOD_MAP = {60: "1m"}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.rate_limits = {}
        self.lock = asyncio.Lock()

    def add_rate_limit(self, total_weight, period):
        self.rate_limits[period] = {
            "total_weight": total_weight,
            "reset_time": period * (datetime.now().timestamp() // period),
            "current_count": 0,
        }

    def check_rate_limit(self, period, count):
        current_count = self.rate_limits[period]["current_count"]
        if count and count > current_count:
            logger.warning(f"Used weight mismatch: {count} > {current_count}")
            self.rate_limits[period]["current_count"] = count
        return current_count

    async def get(self, *args, weight=1, **kwargs):
        while True:
            rate_limit_wait = 0
            async with self.lock:
                for period, limit in self.rate_limits.items():
                    elapsed_time = datetime.now().timestamp() - limit["reset_time"]

                    if elapsed_time > period:
                        limit["current_count"] = 0
                        limit["reset_time"] = period * (
                            datetime.now().timestamp() // period
                        )

                    logger.debug(f"{args} {self.rate_limits}")

                    if (
                        limit["current_count"] + weight
                        >= RateLimitedAsyncClient.RATE_LIMIT_THRESHOLD
                        * limit["total_weight"]
                    ):
                        rate_limit_wait = max(
                            rate_limit_wait,
                            period - datetime.now().timestamp() % period,
                        )
                    else:
                        limit["current_count"] += weight

            if rate_limit_wait == 0:
                break
            else:
                logger.warning(
                    f"Rate limit almost reached, waiting {int(rate_limit_wait)}s..."
                )
                await asyncio.sleep(rate_limit_wait)

        response = await super().get(*args, **kwargs)

        if response.status_code == 429:
            logger.warning(f"Rate limit reached!")

        for period, limit in self.rate_limits.items():
            used_weight = int(
                response.headers[
                    f"x-mbx-used-weight-{RateLimitedAsyncClient.PERIOD_MAP[period]}"
                ]
            )
            self.check_rate_limit(period, used_weight)
            logger.debug(
                f"Used weight({RateLimitedAsyncClient.PERIOD_MAP[period]}): "
                f"{used_weight}/{limit['total_weight']}"
            )
        return response


client = RateLimitedAsyncClient(timeout=None)


async def close():
    await client.aclose()


async def exchange_info():
    global client

    response = await client.get(BASE_URL + "/fapi/v1/exchangeInfo", weight=1)

    rate_limits = response.json()["rateLimits"]
    for limit in rate_limits:
        if limit["rateLimitType"] == "REQUEST_WEIGHT":
            client.add_rate_limit(
                limit["limit"],
                limit["intervalNum"] * {"MINUTE": 60, "SECOND": 1}[limit["interval"]],
            )
        elif limit["rateLimitType"] == "ORDERS":
            # TODO
            pass

    return response.json()


async def continuousKlines(
    pair,
    contract,
    interval="1m",
    start_time=None,
    end_time=None,
    limit=None,
    fetch_all=False,
):
    if interval not in INTERVALS_MAP:
        raise ValueError(
            f"Interval '{interval}' not valid ({', '.join(INTERVALS_MAP.keys())})"
        )

    interval_seconds = INTERVALS_MAP[interval]
    klines = []

    while True:
        logger.debug(f"{pair} klines: {len(klines)}")
        params = {
            "pair": pair,
            "contractType": contract,
            "interval": interval,
        }
        if start_time is not None:
            params.update(startTime=int(start_time * 1000))
        if end_time is not None:
            params.update(endTime=int(end_time * 1000))
        if limit is not None:
            params.update(limit=limit)

        response = await client.get(
            BASE_URL + "/fapi/v1/continuousKlines", params=params, weight=10
        )
        if response.status_code != 200:
            break

        response = response.json()
        klines += response

        if (
            not fetch_all
            or start_time is None
            or len(response) < limit
            or (
                end_time is not None
                and (response[-1][0] // 1000 + interval_seconds > end_time)
            )
        ):
            break
        else:
            start_time = response[-1][0] // 1000 + interval_seconds

    return klines
