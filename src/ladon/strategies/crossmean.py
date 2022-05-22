# Simple Cross sectional mean return strategy
import numpy as np


def returns(candles, window, delta=1):
    closes = np.array([kl.close(slice(-window - delta, None)) for kl in candles])
    return (
        np.diff([closes[:, :-delta], closes[:, delta:]], axis=0)[0] / closes[:, :-delta]
    )


def step(candlesticks, window=None):
    if window is None:
        window = max(len(candle) for candle in candlesticks)

    all_rets = returns(candlesticks, window)
    index_rets = np.nanmean(all_rets, axis=0)
    weights = index_rets - all_rets

    total_sum = np.nansum(np.abs(weights), axis=0)
    weights = np.divide(
        weights, total_sum, out=np.zeros_like(weights), where=total_sum != 0
    )

    return weights
