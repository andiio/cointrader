'''
Helper for manipulating cryptocurrency market data.
'''
import os

__author__ = 'dmertl'

import sqlite3
import datetime
import abc
import pprint
import datetime
import time

# class MarketDataSource
#   Handles retrieval and updating of data

class MarketDataMediator:
    """
    Handles retrieving of market data and converting into an actionable format. Utilizes caching to improve performance.

    input: market data, actionable data format
    output: actionable data

    TODO: caching
        i-mtgox or i-mtgox-btc-usd
        o-candles-mtgox-btc-usd-3600-1325404800-1357027200
    TODO: should we cache the normalized form of the market data?
    """
    def get(self, market_data, actionable_data_formatter):
        """
        :type market_data: MarketData
        :type actionable_data_formatter: ActionableDataFormatter
        :rtype: ActionableData
        """
        #output_key = output_adapter.get_cache_key()
        #if self.cache_exists(output_key):
        #    return self.get_cached(output_key)
        #else:
        #    self.update_market_data(input_adapter.get_source())
        return actionable_data_formatter.process(market_data)


class MarketData(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, source, item, currency, start=None, end=None):
        """
        :param source: Data source for raw market data
        :type source: str
        :param item: Limit to trades of item being sold
        :type item: str
        :param currency: Limit to trades in this currency
        :type currency: str
        :param start: Timestamp of start of desired data range, inclusive
        :type start: int
        :param end: Timestamp of end of desired data range, exclusive
        :type end: int
        """
        self.source = source
        self.item = item
        self.currency = currency
        self.start = start
        self.end = end

    @abc.abstractmethod
    def __iter__(self):
        return


class MarketDataSqlite(MarketData):

    def __init__(self, source, item, currency, start=None, end=None):
        MarketData.__init__(self, source, item, currency, start, end)
        self._conn = None
        self._cursor = None

        self.schema = {
            'table': 'trades',
            'fields': {
                'item': 'item',
                'currency': 'currency',
                'date': 'date',
                'type': 'type',
                'amount': 'amount',
                'price': 'price'
            }
        }

    def __iter__(self):
        return self

    def next(self):
        if not self._conn:
            self._conn = sqlite3.connect(self.source)
            self._conn.row_factory = sqlite3.Row
        if not self._cursor:
            self._cursor = self._conn.cursor()
            self._cursor.execute(self.generate_query(self.item, self.currency, self.start, self.end), {
                'item': self.item,
                'currency': self.currency,
                'start': self.start,
                'end': self.end
            })
        row = self._cursor.fetchone()
        if row:
            return self.row_to_trade(row)
        else:
            raise StopIteration

    def generate_query(self, item, currency, start=None, end=None):
        query = 'SELECT * FROM `{table}` WHERE `{item}` = :item AND `{currency}` = :currency'
        if start:
            query += ' AND `{date}` >= :start'
        if end:
            query += ' AND `{date}` < :end'
        return query.format(table=self.schema['table'], item=self.schema['fields']['item'],
                            currency=self.schema['fields']['currency'], date=self.schema['fields']['date'])

    def row_to_trade(self, row):
        # Convert from DB format to MarketTrade format
        return MarketTrade(
            self.convert_date(row[self.schema['fields']['date']]),
            self.convert_type(row[self.schema['fields']['type']]),
            self.convert_amount(row[self.schema['fields']['amount']]),
            self.convert_price(row[self.schema['fields']['price']])
        )

    def convert_date(self, date):
        return date

    def convert_type(self, type_):
        return type_

    def convert_amount(self, amount):
        return amount

    def convert_price(self, price):
        return price


class MarketDataMtgox(MarketDataSqlite):

    def __init__(self, source, item, currency, start=None, end=None):
        super(MarketDataMtgox, self).__init__(source, item, currency, start, end)

        self.schema['table'] = 'dump'
        self.schema['fields']['item'] = 'Item'
        self.schema['fields']['currency'] = 'Currency__'
        self.schema['fields']['date'] = 'Date'
        self.schema['fields']['type'] = 'Type'
        self.schema['fields']['amount'] = 'Price'
        self.schema['fields']['price'] = 'Amount'

    def generate_query(self, item, currency, start=None, end=None):
        query = super(MarketDataMtgox, self).generate_query(item, currency, start, end)
        # Add requirement that Primary field is true
        #query += ' AND `Primary` IS TRUE'
        return query

    def convert_date(self, date):
        if date:
            return time.mktime(datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S').timetuple())
        else:
            return None

    def convert_type(self, type_):
        if type_ == 'bid':
            return MarketTrade.TYPE_BID
        else:
            return MarketTrade.TYPE_ASK


class MarketTrade:

    TYPE_BID = 1
    TYPE_ASK = 2

    def __init__(self, date, type_, amount, price):
        """

        :param date: Timestamp of date order was placed
        :type date: int
        :param type_: Type of order placed, TYPE_BID or TYPE_ASK
        :type type_: int
        :param amount: Quantity of item purchased
        :type amount: int
        :param price: Price of each item
        :type price: float
        """
        self.date = date
        self.type = type_
        self.amount = amount
        self.price = price


class ActionableDataFormatter(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, settings):
        """
        :param settings: Settings for formatter
        :type settings: dict
        """
        self.settings = settings

    @abc.abstractmethod
    def process(self, market_data):
        """

        :param market_data: Market data
        :type market_data: MarketData
        :return: Actionable data
        :rtype: ActionableData
        """
        return


class ActionableDataFormatterCandles(ActionableDataFormatter):

    def __init__(self, settings):
        """

        :param settings: Setting for formatter
        :type settings: dict
        """
        ActionableDataFormatter.__init__(self, settings)
        if 'interval' not in settings:
            raise ValueError('Missing required setting "interval".')
        self.interval = settings['interval']

    def process(self, market_data):
        """

        :param market_data:
        :type market_data: MarketData
        :return:
        :rtype: ActionableDataCandles
        """
        interval_start = interval_end = None
        candles = []
        bucket = []

        for trade in market_data:
            # Complicated interval calc due to not being able to get first trade without iterating
            if not interval_start:
                if market_data.start:
                    interval_start = market_data.start
                else:
                    interval_start = trade.date
            if not interval_end:
                interval_end = interval_start + self.interval
            # Check if trade is outside of the current bucket interval
            if trade.date >= interval_end:
                # Calculate candle and add to list
                candles.append(self.calculate_candle(interval_start, self.interval, bucket))
                # Empty bucket
                bucket = []
                # Increment interval
                interval_end += self.interval
            # Add trade to bucket
            bucket.append(trade)

        # Add final candle if any trades left in the bucket
        if bucket:
            candles.append(self.calculate_candle(interval_start, self.interval, bucket))

        # Convert to actionable data and return
        return ActionableDataCandles(candles)

    def calculate_candle(self, date, interval, bucket):
        return ActionableDatumCandle(date, interval, bucket[0], max(bucket), min(bucket), bucket[-1])


class ActionableData():

    def __init__(self, data):
        """

        :param data:
        :type data: list
        """
        self._data = data

    def __iter__(self):
        return iter(self._data)


class ActionableDataCandles(ActionableData):

    def __init__(self, data):
        """

        :param data: Actionable data
        :type data: list[ActionableDatumCandle]
        :return:
        :rtype:
        """
        ActionableData.__init__(self, data)


class ActionableDatumCandle():

    def __init__(self, date, interval, open_, high, low, close):
        """

        :param date: Timestamp of start of candle interval, inclusive
        :type date: int
        :param interval: Length of candle interval in seconds, exclusive
        :type interval: int
        :param open_: Opening price
        :type open_: float
        :param high: High price
        :type high: float
        :param low: Low price
        :type low: float
        :param close: Closing price
        :type close: float
        """
        self.date = date
        self.interval = interval
        self.open = open_
        self.high = high
        self.low = low
        self.close = close

if __name__ == '__main__':
    market_data = MarketDataMtgox('market_data/mtgox.db', 'BTC', 'USD')
    formatter = ActionableDataFormatterCandles({"interval": 3600})
    actionable_data = formatter.process(market_data)
    for datum in actionable_data:
        pprint.pprint(actionable_data)
