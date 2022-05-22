# Multi timeframe MA+BB strategy
import numpy as np


def bb_zscore(candles, window, period):
    if period is not None:
        closes = np.array([c.close(slice(-window - period + 1, None)) for c in candles])
        bb_ma = np.array(
            [np.convolve(c, np.ones(period), mode="valid") / period for c in closes]
        )
        bb_std = np.array(
            [
                np.std(closes[:, i - period : i], axis=1)
                for i in range(period, window + period)
            ]
        ).T
        zscore = np.divide(
            closes[:, period - 1 :] - bb_ma,
            bb_std,
            out=np.zeros((len(candles), window)),
            where=bb_std != 0,
        )
    else:
        zscore = 0

    return zscore


def step(
    candlesticks,
    periods=[20, 80, 320, 1280],
    width_coef=4,
    time_coef=0.8,
    window=None,
):
    if window is None:
        window = max(len(candle) for candle in candlesticks)

    partial_weights = []
    coef = 1.0
    for period in periods:
        zscore = bb_zscore(candlesticks, window, period)
        partial_weights.append(coef * (-1 * np.power(zscore, 3) + width_coef * zscore))
        coef *= time_coef

    weights = np.nansum(partial_weights, axis=0)

    weights[weights < 0] = 0

    # Normalize weights
    total_sum = np.sum(np.abs(weights), axis=0)
    weights = np.divide(
        weights, total_sum, out=np.zeros_like(weights), where=total_sum != 0
    )

    return weights
