Feature: Convert input data (trades and candlesticks) into other formats
    Base data may include raw/aggregated trades or low timeframe candlesticks.
    These may be converted to higher period candlesticks and be merged together.

    # Trades are aggregated into candlesticks respecting
    # the open, close, low and high prices,
    # the sum of the volumes and
    # the period in which they happen
    Scenario: Single trade in a period
        Given a trade on a market
            | time      | price | quantity | taker_buy |
            | 1234567890 | 1.01  | 10.0     | false     |
        When we convert this into candlesticks with one day period
        Then the result is the following candlestick
            | time       | open | low  | high | close | volume | buy_volume |
            | 1234483200 | 1.01 | 1.01 | 1.01 | 1.01  | 10.0   | 0.0        |

    Scenario: Four trades in a period
        Given these trades on a market
            | time      | price | quantity | taker_buy |
            | 1234567890 | 1.02 | 10.0    | true      |
            | 1234567891 | 1.01 | 5.5     | false     |
            | 1234567892 | 1.04 | 14.0    | true      |
            | 1234567893 | 1.03 | 8.1     | false     |
        When we convert this into candlesticks with one day period
        Then the result is the following candlestick
            | time       | open | low  | high | close | volume | buy_volume |
            | 1234483200 | 1.02 | 1.01 | 1.04 | 1.03  | 37.6   | 24.0        |

    Scenario: Periods without trades
        Given these trades on a market
            | time      | price | quantity | taker_buy |
            | 1234567890 | 1.01 | 10.0    | false     |
            | 1234656789 | 1.04 | 11.0    | true      |
        When we convert this into candlesticks with one day period
        Then the result is the following candlesticks
            | time       | open | low  | high | close | volume | buy_volume |
            | 1234483200 | 1.01 | 1.01 | 1.01 | 1.01  | 10.0   | 0.0        |
            | 1234569600 | 1.01 | 1.01 | 1.01 | 1.01  | 0.0    | 0.0        |
            | 1234656000 | 1.04 | 1.04 | 1.04 | 1.04  | 11.0   | 11.0       |

    Scenario: Many trades inside a few periods
        Given these trades on a market
            | time      | price | quantity | taker_buy |
            | 1234567890 | 1.02 | 10.0    | true      |
            | 1234567891 | 1.01 | 5.5     | false     |
            | 1234567892 | 1.04 | 14.0    | true      |
            | 1234567893 | 1.02 | 8.1     | false     |
            | 1234567894 | 1.03 | 3.1     | true      |
            | 1234569678 | 1.02 | 1.0     | false     |
            | 1234569679 | 1.03 | 2.0     | true      |
            | 1234569680 | 1.04 | 3.0     | false     |
            | 1234569681 | 1.05 | 4.0     | true      |
        When we convert this into candlesticks with one day period
        Then the result is the following candlestick
            | time       | open | low  | high | close | volume | buy_volume |
            | 1234483200 | 1.02 | 1.01 | 1.04 | 1.03  | 40.7   | 27.1       |
            | 1234569600 | 1.02 | 1.02 | 1.05 | 1.05  | 10.0   | 6.0        |

    Scenario: Streaming of trades/candlesticks
        Given a stream of trades on a market
            | time      | price | quantity | taker_buy |
            | 1234567890 | 1.02 | 10.0    | true      |
            | 1234567891 | 1.01 | 5.5     | false     |
            | 1234567892 | 1.04 | 14.0    | true      |
            | 1234567893 | 1.02 | 8.1     | false     |
            | 1234567894 | 1.03 | 3.1     | true      |
            | 1234569678 | 1.02 | 1.0     | false     |
        When we convert this into a stream of candlesticks with one day period
        Then the result is the following candlestick stream
            | time       | open | low  | high | close | volume | buy_volume |
            | 1234483200 | 1.02 | 1.02 | 1.02 | 1.02  | 10.0   | 10.0       |
            | 1234483200 | 1.02 | 1.01 | 1.02 | 1.01  | 15.5   | 10.0       |
            | 1234483200 | 1.02 | 1.01 | 1.04 | 1.04  | 29.5   | 24.0       |
            | 1234483200 | 1.02 | 1.01 | 1.04 | 1.02  | 37.6   | 24.0       |
            | 1234483200 | 1.02 | 1.01 | 1.04 | 1.03  | 40.7   | 27.1       |
            | 1234569600 | 1.02 | 1.02 | 1.02 | 1.02  | 1.0    | 0.0        |
