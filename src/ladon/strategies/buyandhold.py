# Simple Buy and Hold strategy
import numpy as np


def step(candlesticks, window=None, **kwargs):
    if window is None:
        window = max(len(candlesticks[s]) for s in candlesticks)

    closes = np.array([c.close(slice(-window, None)) for c in candlesticks])
    weights = np.zeros_like(closes)

    weights[~np.isnan(closes)] = 1.0

    total_sum = np.nansum(np.abs(weights), axis=0)
    weights = np.divide(
        weights, total_sum, out=np.zeros_like(weights), where=total_sum != 0
    )

    return weights
