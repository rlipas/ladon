import asyncio
import sys

from .fetch import fetch


def main():
    interval = sys.argv[1].lower()
    symbols = list(map(str.upper, sys.argv[2:]))

    asyncio.run(fetch(interval, symbols))
