from asyncio import run

import ccxt
import ccxt.pro as ccxtpro

import time


async def watch_order_book_for_symbol(exchange, symbol):
    if exchange == "binance":
        exchange = ccxtpro.binance({"newUpdates": False})
    elif exchange == "kraken":
        exchange = ccxtpro.kraken({"newUpdates": False})

    orderbook = await exchange.watch_order_book(symbol)
    print(orderbook)

    # while True:
    #     orderbook = await exchange.watch_order_book(symbol)
    #     print(orderbook)
    # print(orderbook["asks"][0], orderbook["bids"][0])
    await exchange.close()


async def watch_mark_prices(symbols):
    exchange = ccxtpro.binance({"newUpdates": False})

    while True:
        mark_prices = await exchange.watch_mark_prices(symbols)
        print(mark_prices)


async def watch_mark_price(symbol):
    exchange = ccxtpro.binance({"newUpdates": False})

    while True:
        mark_prices = await exchange.watch_mark_price(symbol)
        print(mark_prices)


# streaming WORKING
async def watch_tickers(symbols):
    exchange = ccxtpro.binance({"newUpdates": False})

    while True:
        try:
            tickers = await exchange.watch_tickers(symbols)
            print(exchange.iso8601(exchange.milliseconds()), tickers)
            time.sleep(1)
        except Exception as e:
            print(e)
            # stop the loop on exception or leave it commented to retry
            # raise e
    await exchange.close()


async def main():
    # print(ccxt.exchanges)  # print a list of all available exchange classes

    # await watch_order_book_for_symbol(
    #     "binance", "BTC/USDT"
    # )  # watch BTC/USDT order book on Binance

    # await watch_mark_prices(["BTCUSDT", "ETHUSDT"])  # prices

    # await watch_tickers(["BTC/USDT", "ETH/USDT"])  # tickers

    # await watch_mark_prices(["BTC/USDT", "ETH/USDT"])  # prices
    await watch_mark_price("BTC/USDT")  # prices


run(main())
