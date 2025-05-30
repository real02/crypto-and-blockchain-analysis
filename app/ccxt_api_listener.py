from fastapi import FastAPI
import ccxt

app = FastAPI()

# Initialize exchange instances
exchanges = {
    "binance": ccxt.binance(),
    "kraken": ccxt.kraken(),
    "coinbase": ccxt.coinbase(),
}


@app.get("/orderbook/")
async def get_order_book(symbol: str = "BTC/USDT", limit: int = 5):
    order_books = {}

    for name, exchange in exchanges.items():
        try:
            orderbook = exchange.fetch_order_book(symbol, limit)
            order_books[name] = {
                "bids": orderbook["bids"],
                "asks": orderbook["asks"],
                "timestamp": orderbook["timestamp"],
            }
        except Exception as e:
            order_books[name] = {"error": str(e)}

    return order_books


# Run with: uvicorn filename:app --reload
