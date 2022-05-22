import numpy as np

INTERVALS_MAP = {
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


class Candlestick(object):
    def __init__(self, symbol, sampling=None, max_history=None, init=None):
        self.symbol = symbol
        self.sampling = sampling
        self.data = {}  # (time, open, high, low, close, volume, quote volume)
        self.sorted_times = []
        self.max_history = max_history
        if init is not None:
            for candle in init:
                self.append_candlestick(candle)
        self._array = None

    def resample(self, sampling):
        if self.sampling != sampling:
            self.sampling = sampling
            self._array = None

    def update_candlestick(self, data):
        if "v2" not in data:
            data["v2"] = data["v"] * data["c"]
        if any(data[i] == 0 for i in ["o", "h", "l", "c", "v", "v2"]):
            return

        if data["id"] in self.data:
            self.data[data["id"]] = (
                data["id"],
                data["o"],
                data["h"],
                data["l"],
                data["c"],
                data["v"],
                data["v2"],
            )
        else:
            self.append_candlestick(data)

        self._array = None

    def append_candlestick(self, data):
        if "v2" not in data:
            data["v2"] = data["v"] * data["c"]
        if any(data[i] == 0 for i in ["o", "h", "l", "c", "v", "v2"]):
            return

        if data["id"] not in self.data:
            self.sorted_times.append(data["id"])
            if (
                len(self.sorted_times) > 1
                and self.sorted_times[-1] < self.sorted_times[-2]
            ):
                self.sorted_times.sort()

        self.data[data["id"]] = (
            data["id"],
            data["o"],
            data["h"],
            data["l"],
            data["c"],
            data["v"],
            data["v2"],
        )

        if self.max_history is not None and len(self.data) > self.max_history:
            del self.data[self.sorted_times[0]]
            self.sorted_times.pop(0)

        self._array = None

    def append_trade(self, data):
        if data["ts"] // 1000 not in self.data:
            self.sorted_times.append(data["ts"] // 1000)
            if (
                len(self.sorted_times) > 1
                and self.sorted_times[-1] < self.sorted_times[-2]
            ):
                self.sorted_times.sort()

        self.data[data["ts"] // 1000] = (
            data["ts"] // 1000.0,
            data["px"],
            data["px"],
            data["px"],
            data["px"],
            data["qty"],
            data["qty"] * data["px"],
        )

        if self.max_history is not None and len(self.data) > self.max_history:
            del self.data[self.sorted_times[0]]
            self.sorted_times.pop(0)

        self._array = None

    def __getitem__(self, key):
        if self.sampling is None:
            keys = self.sorted_times[key]
            return (
                [self.data[k] for k in keys] if type(keys) == list else self.data[keys]
            )
        else:
            return list(iter(self))[key]

    def __len__(self):
        if self.sampling is None or len(self.data) == 0:
            return len(self.data)
        else:
            return (
                self.sorted_times[-1] // self.sampling
                - self.sorted_times[0] // self.sampling
            )

    def __iter__(self):
        if self.sampling is None:
            yield from (self.data[t] for t in self.sorted_times)
        else:
            initial_time = int(self.sorted_times[0] // self.sampling) * self.sampling
            next_time = initial_time + self.sampling
            current_candle = list(self.data[self.sorted_times[0]])
            current_candle[0] = initial_time
            i = 0
            while i < len(self.sorted_times) - 1:
                if (
                    self.data[self.sorted_times[i + 1]][0] >= next_time
                    and self.data[self.sorted_times[i + 1]][0]
                    < next_time + self.sampling
                ):
                    yield current_candle
                    i += 1
                    current_candle = list(self.data[self.sorted_times[i]])
                    current_candle[0] = next_time
                    next_time += self.sampling
                elif (
                    self.data[self.sorted_times[i + 1]][0] >= next_time + self.sampling
                ):
                    yield current_candle
                    current_candle[0] = next_time
                    current_candle[1] = current_candle[4]
                    current_candle[2] = current_candle[4]
                    current_candle[3] = current_candle[4]
                    current_candle[5] = 0
                    current_candle[6] = 0
                    next_time += self.sampling
                else:
                    i += 1
                    data = self.data[self.sorted_times[i]]
                    current_candle[2] = max(current_candle[2], data[2])
                    current_candle[3] = min(current_candle[3], data[3])
                    current_candle[4] = data[4]
                    current_candle[5] += data[5]
                    current_candle[6] += data[6]

            yield current_candle

    @property
    def np_array(self):
        if len(self.data) == 0:
            self._array = np.empty((0, 7))
        elif self._array is None:
            self._array = np.array(self)
        return self._array

    def _nanpadl_slice(self, key, series):
        key = key if type(key) == slice else (slice(None) if key is None else key)
        if type(key) != slice or key.start is None:
            return self.np_array[key, series]

        total_size = -key.start
        return np.pad(
            self.np_array,
            ((max(total_size - self.np_array.shape[0], 0), 0), (0, 0)),
            constant_values=np.nan,
        )[key, series]

    def time(self, key=None):
        return self._nanpadl_slice(key, 0)

    def open(self, key=None):
        return self._nanpadl_slice(key, 1)

    def high(self, key=None):
        return self._nanpadl_slice(key, 2)

    def low(self, key=None):
        return self._nanpadl_slice(key, 3)

    def close(self, key=None):
        return self._nanpadl_slice(key, 4)

    def vol(self, key=None):
        return self._nanpadl_slice(key, 5)

    def vol2(self, key=None):
        return self._nanpadl_slice(key, 6)
