import argparse
import asyncio
import logging

from .fetch import fetch


def main():
    parser = argparse.ArgumentParser(
        description="Ladon is a framework for backtesting and running algorithmic trading bots."
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="verbose output (multiple for even more verbose)",
    )

    subparsers = parser.add_subparsers(title="subcommands")

    # Fetch subcommand
    parser_fetch = subparsers.add_parser(
        "fetch", help="fetch historical data for backtesting"
    )
    parser_fetch.add_argument(
        "-p", "--provider", help="historical data provider", required=True
    )
    parser_fetch.add_argument(
        "-i",
        "--interval",
        help="candlesticks interval (default: %(default)s)",
        default="1d",
    )
    parser_fetch.add_argument(
        "-s", "--symbols", help="symbols to fetch (default: all)", nargs="*"
    )
    parser_fetch.set_defaults(func=fetch_main)

    # Backtest subcommand
    parser_backtest = subparsers.add_parser(
        "backtest", help="Backtest strategy using historical data"
    )
    parser_backtest.add_argument(
        "-s", "--strategy", help="strategy to use", required=True
    )
    parser_backtest.set_defaults(func=backtest_main)

    # Forward test subcommand
    parser_forwardtest = subparsers.add_parser(
        "forwardtest", help="Forward test strategy using data provider"
    )
    parser_forwardtest.add_argument(
        "-s", "--strategy", help="strategy to use", required=True
    )
    parser_forwardtest.add_argument(
        "-p", "--provider", help="Exchange provider name", required=True
    )
    parser_forwardtest.set_defaults(func=forwardtest_main)

    # Trade subcommand
    parser_trade = subparsers.add_parser(
        "trade", help="Run strategy live on a exchange"
    )
    parser_trade.add_argument("-s", "--strategy", help="strategy to use", required=True)
    parser_trade.add_argument(
        "-p", "--provider", help="Exchange provider name", required=True
    )
    parser_trade.set_defaults(func=trade_main)

    args = parser.parse_args()
    logging.basicConfig(
        level=[logging.ERROR, logging.WARNING, logging.INFO, logging.DEBUG][
            min(3, args.verbose)
        ]
    )
    if "func" in args:
        args.func(args)
    else:
        parser.print_usage()


def fetch_main(args):
    provider = args.provider
    interval = args.interval
    symbols = list(map(str.upper, args.symbols)) if args.symbols else None

    asyncio.run(fetch(provider, interval, symbols))


def backtest_main(args):
    raise NotImplementedError("Backtesting not implemented yet!")


def forwardtest_main(args):
    raise NotImplementedError("Backtesting not implemented yet!")


def trade_main(args):
    raise NotImplementedError("Live trading not implemented yet!")


if __name__ == "__main__":
    main()
