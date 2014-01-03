cointrader
==========

Summary
-------

Cryptocurrency trading bot.

Features
--------

- Test strategies against historical data
- Modular strategy architecture
- Interface with market APIs to automatically place orders
- Cross currency market strategies?

Modular Strategy Architecture
-----------------------------

### Input

- Market pricing data
 - Over what kind of time span? May want to put a max limit on range.
 - What kind of time interval is data in? Availability may vary from market to market.
 - Use candle format from gekko / nitrous data tool
- Configuration
 - Values for strategy's configuration settings

### Output

- Purchase order
 - BUY, SELL, or HOLD
 - What do we do about cancelling BUY or SELL orders? If output switches cancel all previous orders?
 - How often do we issue orders?
 - How long does it take for an order to register on the market?
- Price
 - How do we determine the price? One significant digit above/below market price?
- Quantity?
 - If not part of the algorithm, maybe make each order a fixed % of total assets. Or take quantity outside of strategy.

### Analysis

- Run strategy over a fixed length of time on historical data.
- Calculate net profit.
- Save strategy settings and log of all orders.
- Some kind of automation to tweak settings and re-run strategy.
