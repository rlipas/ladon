import bz2
import logging
from importlib import import_module

import numpy as np
import ujson

from ladon.base import Candlestick

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


def load_all(filename):
    candlesticks = []
    with bz2.open(filename, mode="r") as f:
        info = ujson.load(f)
        for symbol in info["symbols"]:
            if "klines" in symbol:
                logger.debug(f'{symbol["symbol"]} {len(symbol["klines"])}')
                candlesticks.append(load(symbol["symbol"], symbol["klines"]))
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


def backtest(data_file, strategy_name):
    strategy = import_module(f"ladon.strategies.{strategy_name}")
    candlesticks = load_all(data_file)
    window = max(len(candle) for candle in candlesticks)

    weights = strategy.step(candlesticks)

    symbols_returns = returns(candlesticks, window)
    strategy_returns = np.sum(symbols_returns * shift(weights, 1, 0), axis=0)[1:]

    print(f"Cumulative returns: {np.sum(strategy_returns)}")
