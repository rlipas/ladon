import logging
from importlib import import_module

import matplotlib.pyplot as plt
import numpy as np

from ladon.base import Candlestick
from ladon.database import SqliteDatabase

logger = logging.getLogger(__name__)


def load(symbol, data):
    candlestick = Candlestick(symbol)
    for line in data:
        candlestick.append_candlestick(
            {
                "id": line[0] // 1000,
                "o": float(line[1]),
                "h": float(line[2]),
                "l": float(line[3]),
                "c": float(line[4]),
                "v": float(line[5]),
            }
        )
    return candlestick


def load_all(database, provider_name, period, symbols=None):
    candlesticks = []

    info = database.get_provider_info(provider_name)
    for symbol in info["symbols"]:
        if symbols is None or symbol["symbol"] in symbols:
            candles = database.get_candlesticks(provider_name, symbol["symbol"], period)
            if len(candles) > 0:
                candlesticks.append(load(symbol["symbol"], candles))

    return candlesticks


def returns(candlesticks, window):
    closes = np.array(
        [candle.close(slice(-window - 1, None)) for candle in candlesticks]
    )
    return np.diff(closes, axis=1) / closes[:, :-1]


def shift(arr, num, fill_value=np.nan):
    result = np.empty_like(arr)
    if num > 0:
        result[:, :num] = fill_value
        result[:, num:] = arr[:, :-num]
    elif num < 0:
        result[:, num:] = fill_value
        result[:, :num] = arr[:, -num:]
    else:
        result[:] = arr
    return result


def backtest(provider_name, period, strategy_name, symbols=None):
    strategy = import_module(f"ladon.strategies.{strategy_name}")
    db = SqliteDatabase()
    candlesticks = load_all(db, provider_name, period, symbols)
    window = max(len(candle) for candle in candlesticks)

    weights = strategy.step(candlesticks)

    symbols_returns = returns(candlesticks, window)
    strategy_returns = np.nansum(symbols_returns * shift(weights, 1, 0), axis=0)[1:]

    plt.plot(np.cumprod(1 + strategy_returns) - 1)
    plt.show()

    print(f"Cumulative returns: {np.nanprod(1+strategy_returns)-1}")
