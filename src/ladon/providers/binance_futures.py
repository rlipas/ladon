import logging

import httpx

from ladon.base import INTERVALS_MAP

logger = logging.getLogger(__name__)

BASE_URL = "https://fapi.binance.com"


async def exchange_info():
    async with httpx.AsyncClient() as client:
        response = await client.get(BASE_URL + "/fapi/v1/exchangeInfo")

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

        async with httpx.AsyncClient() as client:
            response = await client.get(
                BASE_URL + "/fapi/v1/continuousKlines", params=params
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
