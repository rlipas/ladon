import sys

import httpx

BASE_URL = "https://fapi.binance.com"

interval_map = {
    "1m": 60,
    "3m": 3 * 60,
    "5m": 5 * 60,
    "15m": 15 * 60,
    "30m": 30 * 60,
    "1h": 60 * 60,
    "2h": 2 * 60 * 60,
    "4h": 4 * 60 * 60,
    "6h": 6 * 60 * 60,
    "8h": 8 * 60 * 60,
    "12h": 12 * 60 * 60,
    "1d": 24 * 60 * 60,
    "3d": 3 * 24 * 60 * 60,
    "1w": 7 * 24 * 60 * 60,
}


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
    if interval not in interval_map:
        print(f"Interval '{interval}' not valid ({', '.join(interval_map.keys())})")
        sys.exit(1)

    interval_seconds = interval_map[interval]
    klines = []

    while True:
        print(
            f"{pair}: {len(klines)}    ", file=sys.stderr, end="\r",
        )
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
