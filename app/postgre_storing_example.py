import psycopg2

conn = psycopg2.connect("dbname=yourdb user=youruser password=yourpass")
cur = conn.cursor()


def save_prices(prices):
    for exchange, data in prices.items():
        cur.execute(
            "INSERT INTO price_snapshot (exchange_id, asset, bid_price, ask_price) VALUES ((SELECT id FROM exchange WHERE name=%s), %s, %s, %s)",
            (exchange, "BTC/USDT", data["bid"], data["ask"]),
        )
    conn.commit()


while True:
    prices = fetch_prices()
    save_prices(prices)
    time.sleep(1)
