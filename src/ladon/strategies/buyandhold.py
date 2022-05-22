# Simple MA Cross strategy
import numpy as np


def step(candlesticks):
    return np.ones((len(candlesticks), len(candlesticks[0]))) / len(candlesticks)
