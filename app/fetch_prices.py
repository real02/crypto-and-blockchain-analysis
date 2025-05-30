import ccxt
import time

# Initialize exchanges
exchanges = {
    "binance": ccxt.binance(),
    "kraken": ccxt.kraken(),
    "coinbase": ccxt.coinbase(),
}

symbol = "BTC/USDT"


def fetch_bid_ask_prices():
    prices = {}
    for name, exchange in exchanges.items():
        try:
            ticker = exchange.fetch_ticker(symbol)
            print(f"{name}:", ticker)
            prices[name] = {
                "bid": ticker["bid"],
                "ask": ticker["ask"],
                "timestamp": ticker["timestamp"],
            }
        except Exception as e:
            print(f"Error fetching {name}: {e}")
    return prices


while True:
    prices = fetch_bid_ask_prices()
    print("Live Bid/Ask:", prices)
    time.sleep(1)  # Respect rate limits
