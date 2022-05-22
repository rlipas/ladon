# Simple MA Cross strategy
import numpy as np


def step(candlesticks, fast_length=10, slow_length=20):
    return np.ones((len(candlesticks), len(candlesticks[0])))
