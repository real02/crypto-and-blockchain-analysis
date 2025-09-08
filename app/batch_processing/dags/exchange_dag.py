import asyncio
import logging
import pendulum
import snowflake.connector

from airflow.decorators import dag, task
from airflow.sdk import Variable
from datetime import timedelta

from fastapi_listener import rate_limited_fetch_exchange_data

SYMBOLS = ['BTC/USDT', 'ETH/USDT']
EXCHANGES = ['binance', 'kraken', 'coinbasepro']

default_args ={
    'owner': 'aiflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

def load_to_snowflake(records):
    if not records:
        logging.warning("No data to load into Snowflake.")
    conn = snowflake.connector.connect(**Variable.get("snowflake_conn_json", deserialize_json=True))
    cs = conn.cursor()
    insert_stmt = """
        INSERT INTO cex_prices_raw (ts, exchange, symbol, bid, ask, last, bid_volume, ask_volume, spread, spread_percentage) 
        SELECT to_timestamp_ltz(%s), %s, %s, %s, %s, %s, %s, %s, %s, %s
    """
 
    for r in records:
        ts = r['timestamp']
        cs.execute(insert_stmt, (
            ts,
            r['exchange'],
            r['symbol'],
            r['bid'],
            r['ask'],
            r['last'],
            r['bid_volume'],
            r['ask_volume'],
            r['spread'],
            r['spread_percentage']
        ))

    cs.close()
    conn.close()
    logging.info(f"Inserted {len(records)} records into Snowflake.")


def run_async_cex_fetch_and_load():
    async def collect_all():
        tasks = [rate_limited_fetch_exchange_data(exchange, SYMBOLS) for exchange in EXCHANGES]
        results = await asyncio.gather(*tasks)
        return [record for sublist in results for record in sublist]

    records = asyncio.run(collect_all())
    load_to_snowflake(records)



@dag(
    dag_id='cex_price_fetch_and_load_dag',
    default_args=default_args,
    schedule='*/30 * * * *',
    start_date=pendulum.now('UTC').subtract(days=1),
    catchup=False,
    max_active_runs=1,
    tags=['crypto', 'snowflake']
)
def cex_price_fetch_and_load_dag():
    @task(task_id='fetch_and_load_cex_data')
    def fetch_and_load_cex_data():
        run_async_cex_fetch_and_load()

    fetch_and_load_cex_data()

dag = cex_price_fetch_and_load_dag()

