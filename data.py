'''
Helper for manipulating cryptocurrency market data.
'''
__author__ = 'dmertl'


'''
Convert market data into candles for use in cointrader.
'''
class Candelizer:

    def candelize(self, interval, input_data, output, start=None, end=None):
        """
        :param interval:
        :type interval: int
        :param input_data:
        :type input_data: MarketData
        """
        trades = input_data.trades

        # Default start and end times
        if start is None:
            start = trades[0].date
        if end is None:
            end = trades[-1].date + 1

        # Find next bucket time
        current_bucket_time = start
        next_bucket_time = current_bucket_time + interval
        candles = []
        current_bucket = []

        # Group trades into interval sized buckets and calculate candles (one candle per bucket)
        for trade in trades:
            if trade.date >= next_bucket_time:
                candles.append(self.calculate_candle(current_bucket_time, current_bucket))
                current_bucket = []
                current_bucket_time = next_bucket_time
                next_bucket_time = current_bucket_time + interval
            if trade.date > end:
                break
            current_bucket.append(trade.price)

        # Add candle for final bucket
        if current_bucket:
            candles.append(self.calculate_candle(current_bucket_time, current_bucket))

    def calculate_candle(self, date, bucket):
        return Candle(date, bucket[0], max(bucket), min(bucket), bucket[-1])

'''
Holds market data in normalized format.
'''
class MarketData:

    def __init__(self, trades):
        """
        :param trades:
        :type trades: list[MarketTrade]
        """
        self.trades = trades


class MarketTrade:

    def __init__(self, date, order, price):
        """

        :param date:
        :type date: int
        :param order:
        :type order: float
        :param price:
        :type price: float
        """

        self.date = date
        self.order = order
        self.price = price


# Needs interval info? How do we pick what date is since we cover an interval? Start?
class Candle:

    def __init__(self, date, open, high, low, close):
        self.date = date
        self.open = open
        self.high = high
        self.low = low
        self.close = close

class MarketDataInputAdapter:


    def __init__(self, source):
        self.source = source

'''
Read market data from Mt. Gox SQLite dump.
'''
class MarketDataInputFormatMtGox(MarketDataInputAdapter):

    def readSource(self):
        self.data = source.read

'''
Output market data in CSV candles.
'''
class MarketDataOutputFormatCandles:

    def __init__(self):
