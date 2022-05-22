import json
import sqlite3


class SqliteDatabase(object):
    def __init__(self, filename="ladon.db"):
        self.connection = sqlite3.connect(filename)
        c = self.connection.cursor()
        c.execute(
            "CREATE TABLE IF NOT EXISTS providers ("
            "name TEXT UNIQUE NOT NULL,"
            "info JSON NOT NULL)"
        )
        c.execute(
            "CREATE TABLE IF NOT EXISTS candlesticks ("
            "timestamp INTEGER NOT NULL,"
            "symbol TEXT NOT NULL,"
            "interval TEXT NOT NULL,"
            "provider TEXT NOT NULL REFERENCES providers(name),"
            "data JSON NOT NULL,"
            "PRIMARY KEY (timestamp, symbol, interval, provider))"
        )
        c.execute(
            "CREATE INDEX IF NOT EXISTS candlestick_timestamp_idx "
            "ON candlesticks (timestamp)"
        )

    def get_provider_info(self, name):
        c = self.connection.cursor()
        c.execute("SELECT info from providers where name=:name", {"name": name})
        info = c.fetchone()
        return json.loads(info[0]) if info is not None else None

    def set_provider_info(self, name, info):
        c = self.connection.cursor()
        c.execute(
            "INSERT OR REPLACE INTO providers VALUES (:name, :info)",
            {"name": name, "info": json.dumps(info)},
        )
        self.connection.commit()

    def add_candlestick(self, provider, symbol, interval, timestamp, data):
        c = self.connection.cursor()
        c.execute(
            "INSERT OR REPLACE INTO candlesticks VALUES "
            "(:timestamp, :symbol, :interval, :provider, :data)",
            {
                "timestamp": timestamp,
                "symbol": symbol,
                "interval": interval,
                "provider": provider,
                "data": json.dumps(data),
            },
        )
        self.connection.commit()

    def add_candlesticks(self, provider, symbol, interval, timestamped_data):
        values = [
            {
                "timestamp": timestamp,
                "symbol": symbol,
                "interval": interval,
                "provider": provider,
                "data": json.dumps(data),
            }
            for (timestamp, data) in timestamped_data.items()
        ]
        c = self.connection.cursor()
        c.executemany(
            "INSERT OR REPLACE INTO candlesticks VALUES "
            "(:timestamp, :symbol, :interval, :provider, :data)",
            values,
        )
        self.connection.commit()

    def get_candlestick(self, provider, symbol, interval, timestamp=None, extract=None):
        col_spec = "data" if extract is None else f"json_extract(data, '{extract}')"

        c = self.connection.cursor()
        if timestamp is not None:
            c.execute(
                f"SELECT {col_spec} from candlesticks where "
                "timestamp=:timestamp AND symbol=:symbol AND "
                "interval=:interval AND provider=:provider",
                {
                    "timestamp": timestamp,
                    "symbol": symbol,
                    "interval": interval,
                    "provider": provider,
                },
            )
        else:
            c.execute(
                f"SELECT {col_spec} from candlesticks where "
                "symbol=:symbol AND interval=:interval AND provider=:provider "
                "ORDER BY timestamp DESC LIMIT 1",
                {"symbol": symbol, "interval": interval, "provider": provider},
            )
        result = c.fetchone()
        return (
            (json.loads(result[0]) if extract is None else result[0])
            if result is not None
            else None
        )

    def get_candlesticks(
        self, provider, symbol, interval, start=None, end=None, extract=None
    ):
        col_spec = "data" if extract is None else f"json_extract(data, '{extract}')"
        interval_filter = []
        if start:
            interval_filter.push("interval >= :start")
        if end:
            interval_filter.push("interval <= :end")
        interval_filter = (
            ("AND " + " AND ".join(interval_filter)) if len(interval_filter) > 0 else ""
        )

        c = self.connection.cursor()
        c.execute(
            f"SELECT {col_spec} from candlesticks where "
            "symbol=:symbol AND interval=:interval AND "
            f"provider=:provider {interval_filter} ORDER BY TIMESTAMP",
            {
                "start": start,
                "end": end,
                "symbol": symbol,
                "interval": interval,
                "provider": provider,
            },
        )
        result = c.fetchall()
        return [(json.loads(r[0]) if extract is None else r[0]) for r in result]

    def __del__(self):
        self.connection.close()
