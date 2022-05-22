# Simple MA Cross strategy
import numpy as np


def step(candlesticks, fast_length=20, slow_length=200):
    window = max(len(candle) for candle in candlesticks)
    ma_fast = np.array(
        [
            np.convolve(
                c.close(slice(-window - fast_length + 1, None)),
                np.ones(fast_length),
                mode="full",
            )[:window]
            / fast_length
            for c in candlesticks
        ]
    )
    ma_slow = np.array(
        [
            np.convolve(
                c.close(slice(-window - slow_length + 1, None)),
                np.ones(slow_length),
                mode="full",
            )[:window]
            / slow_length
            for c in candlesticks
        ]
    )
    weights = np.ones((len(candlesticks), len(candlesticks[0]))) / len(candlesticks)
    weights[ma_fast < ma_slow] = 0.0

    # Normalize weights
    weights = np.divide(
        weights,
        np.sum(weights, axis=0),
        out=np.zeros_like(weights),
        where=np.sum(weights, axis=0) != 0,
    )

    return weights
