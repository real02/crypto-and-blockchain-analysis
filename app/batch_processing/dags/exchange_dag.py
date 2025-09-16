import asyncio
import json
import logging
import pendulum
import snowflake.connector

from airflow.decorators import dag, task
from airflow.sdk import Variable
from datetime import datetime, timedelta, timezone

from batch_processing.dags.utils.fetch_loop import fetch_loop
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
        INSERT INTO raw.cex_prices (payload, ingestion_ts) 
        SELECT PARSE_JSON(%s), %s
    """
 
    for r in records:
        json_record = json.dumps(
            {
                "timestamp": r['timestamp'],
                "exchange": r['exchange'],
                "symbol": r['symbol'],
                "bid": r['bid'],
                "ask": r['ask'],
                "last": r['last'],
                "bid_volume": r['bid_volume'],
                "ask_volume": r['ask_volume'],
                "spread": r['spread'],
                "spread_percentage": r['spread_percentage']
            }
        )
        ingestion_time = datetime.now(timezone.utc)
        cs.execute(insert_stmt, (json_record, ingestion_time))

    cs.close()
    conn.close()
    logging.info(f"Inserted {len(records)} records into Snowflake.")


def run_async_cex_fetch_and_load():
    async def run():
        params = [(exchange, SYMBOLS) for exchange in EXCHANGES]
        results = await fetch_loop(rate_limited_fetch_exchange_data, params)
        return results

    records = asyncio.run(run())
    load_to_snowflake(records)



@dag(
    dag_id='cex_price_fetch_and_load_dag',
    default_args=default_args,
    schedule='*/15 * * * *',
    start_date=pendulum.now('UTC').subtract(days=1),
    catchup=False,
    max_active_runs=1,
    tags=['crypto', 'snowflake']
)
def cex_price_fetch_and_load_dag():
    @task(task_id='create_cex_prices_table_if_not_exists')
    def create_cex_prices_table_if_not_exists():
        conn = snowflake.connector.connect(
            **Variable.get("snowflake_conn_json", deserialize_json=True)
        )
        cs = conn.cursor()
        cs.execute("CREATE SCHEMA IF NOT EXISTS raw")
        cs.execute("""
            CREATE TABLE IF NOT EXISTS raw.cex_prices (
                payload VARIANT,
                ingestion_ts TIMESTAMP_NTZ
            )
        """)
        cs.close()
        conn.close()

    @task(task_id='fetch_and_load_cex_data')
    def fetch_and_load_cex_data():
        run_async_cex_fetch_and_load()

    create_cex_prices_table_if_not_exists() >> fetch_and_load_cex_data() # pyright: ignore[reportUnusedExpression]

dag = cex_price_fetch_and_load_dag()

