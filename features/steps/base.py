from ladon.base import (
    Trade,
    Candlestick,
    trades_to_candlesticks,
    trades_to_candlesticks_stream,
)
from decimal import Decimal


@given(u"a trade on a market")
@given(u"these trades on a market")
def step_impl(context):
    context.trades = [
        Trade(int(row[0]), Decimal(row[1]), Decimal(row[2]), row[3].lower() == "true")
        for row in context.table
    ]


@when(u"we convert this into candlesticks with one day period")
def step_impl(context):
    context.result = trades_to_candlesticks(context.trades, 24 * 60 * 60)


@then(u"the result is the following candlestick")
@then(u"the result is the following candlesticks")
def step_impl(context):
    candlesticks = [
        Candlestick(
            int(row[0]),
            Decimal(row[1]),
            Decimal(row[2]),
            Decimal(row[3]),
            Decimal(row[4]),
            Decimal(row[5]),
            Decimal(row[6]),
        )
        for row in context.table
    ]
    assert candlesticks == context.result


@given(u"a stream of trades on a market")
def step_impl(context):
    context.trade_stream = iter(
        [
            Trade(
                int(row[0]), Decimal(row[1]), Decimal(row[2]), row[3].lower() == "true"
            )
            for row in context.table
        ]
    )


@when(u"we convert this into a stream of candlesticks with one day period")
def step_impl(context):
    context.result = trades_to_candlesticks_stream(context.trade_stream, 24 * 60 * 60)


@then(u"the result is the following candlestick stream")
def step_impl(context):
    candlesticks = [
        Candlestick(
            int(row[0]),
            Decimal(row[1]),
            Decimal(row[2]),
            Decimal(row[3]),
            Decimal(row[4]),
            Decimal(row[5]),
            Decimal(row[6]),
        )
        for row in context.table
    ]
    result = list(context.result)
    assert candlesticks == result
