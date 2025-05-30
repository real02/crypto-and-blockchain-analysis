from fetch_prices import fetch_prices


def detect_arbitrage(prices):
    opportunities = []
    sorted_exchanges = sorted(
        prices.items(), key=lambda x: x[1]["ask"]
    )  # Cheapest first
    for buy_exchange, buy_data in sorted_exchanges:
        for sell_exchange, sell_data in sorted_exchanges[::-1]:  # Most expensive first
            if buy_exchange != sell_exchange:
                spread = (sell_data["bid"] - buy_data["ask"]) / buy_data["ask"] * 100
                if spread > 0.5:  # Set a meaningful threshold
                    opportunities.append(
                        {
                            "buy": buy_exchange,
                            "sell": sell_exchange,
                            "buy_price": buy_data["ask"],
                            "sell_price": sell_data["bid"],
                            "spread": spread,
                        }
                    )
    return opportunities


while True:
    prices = fetch_prices()
    arbitrage_opportunities = detect_arbitrage(prices)
    if arbitrage_opportunities:
        print("Arbitrage Opportunities:", arbitrage_opportunities)
    time.sleep(1)
