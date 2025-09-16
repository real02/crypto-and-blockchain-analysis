import aiohttp
import asyncio
import json
import logging
import os
import pandas as pd
import pendulum
import snowflake.connector

from datetime import datetime, timezone

from dotenv import load_dotenv
from pathlib import Path

from airflow.decorators import dag, task
from airflow.sdk import Variable
from batch_processing.dags.utils.fetch_loop import fetch_loop
from batch_processing.snowflake.snowflake_utils import SNOWFLAKE_CONN
from blockchain_collector import etherscan_limiter

from datetime import timedelta

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("etl-dag")

# TODO load these variables from .env file or from Airflow variables
SYMBOLS = ["BTC/USDT", "ETH/USDT"]
EXCHANGES = ["binance", "kraken", "coinbasepro"]

default_args ={
    'owner': 'aiflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}


async def fetch_all_token_transfers(
    contract_address, wallet, start_block=None, end_block=None
):
    ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "")
    if not ETHERSCAN_API_KEY:
        logger.warning(
            "No Etherscan API key found. Using API without key may result in rate limiting."
        )

    page = 1
    per_page = 100
    all_results = []

    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=15)
    ) as session:
        while True:
            await etherscan_limiter.wait()

            url = (
                f"https://api.etherscan.io/api?module=account&action=tokentx"
                f"&contractaddress={contract_address}&address={wallet}"
                f"&page={page}&offset={per_page}&sort=asc&apikey={ETHERSCAN_API_KEY}"
            )
            if start_block:
                url += f"&startblock={start_block}"
            if end_block:
                url += f"&endblock={end_block}"

            async with session.get(url) as res:
                response = await res.json()

            results = response.get("result", [])
            if not results:
                break

            all_results.extend(results)
            page += 1

            if len(results) < per_page:
                break

        return all_results


# hard-coded for the ease of use, in the future will load from snowflake and this code will be deleted anyway
def load_cex_addresses_from_csv():
    path = Path("../dbt/exchange_wallets.csv")
    df = pd.read_csv(path)

    # Clean and normalize addresses
    df["address"] = df["address"].str.lower().str.strip()

    return df["address"].tolist()


def load_usd_weth_data(records):
    conn = snowflake.connector.connect(**Variable.get("snowflake_conn_json", deserialize_json=True), schema="RAW")
    cs = conn.cursor()
    insert_stmt = """
        INSERT INTO raw.token_transfers (payload, ingestion_ts)
        SELECT PARSE_JSON(%s), %s
    """

    for r in records:
        json_record = json.dumps(
            {
                "token": r["token"],
                "tx_hash": r["tx_hash"],
                "from_addr": r["from_addr"],
                "to_addr": r["to_addr"],
                "value": r["value"],
                "time_stamp": int(r["time_stamp"]),
                "block_number": int(r["block_number"]),
            }
        )
        ingestion_time = datetime.now(timezone.utc)
        cs.execute(insert_stmt, (json_record, ingestion_time))
    cs.close()
    conn.close()
    logging.info(f"Inserted {len(records)} records into Snowflake.")


async def fetch_usdt_weth_transfers(contract_address, wallet, start_block, end_block, token):
    txs = await fetch_all_token_transfers(contract_address, wallet, start_block, end_block)
    return [
        {
            "token": token,
            "tx_hash": tx["hash"],
            "from_addr": tx["from"].lower(),
            "to_addr": tx["to"].lower(),
            "value": tx["value"],
            "time_stamp": tx["timeStamp"],
            "block_number": tx["blockNumber"],
        }
        for tx in txs
    ]


def run_async_fetch_and_load_usdt_weth(**ctx):
    async def run():
        start_block = ctx.get("params", {}).get("start_block")
        end_block = ctx.get("params", {}).get("end_block")

        USDT_ADDRESS = os.getenv("USDT_ADDRESS", "")
        if not USDT_ADDRESS:
            logger.warning(
                "No USDT address found. Please provide USDT_ADDRESS in your environment configuration."
            )

        WETH_ADDRESS = os.getenv("WETH_ADDRESS", "")
        if not WETH_ADDRESS:
            logger.warning(
                "No WETH address found. Please provide WETH_ADDRESS in your environment configuration."
            )

        CEX_ADDRESSES = load_cex_addresses_from_csv()

        params = []

        for token, addr in [("usdt", USDT_ADDRESS), ("weth", WETH_ADDRESS)]:
            for wallet in CEX_ADDRESSES:
                params.append((addr, wallet, start_block, end_block, token))

        return await fetch_loop(fetch_usdt_weth_transfers, params)

    records = asyncio.run(run())
    load_usd_weth_data(records)


def load_gas_prices_data(records):
    insert_stmt = """
        INSERT INTO raw.gas_prices (payload, ingestion_ts) 
        SELECT PARSE_JSON(%s), %s
    """

    conn = snowflake.connector.connect(**Variable.get("snowflake_conn_json", deserialize_json=True), schema="RAW")
    cs = conn.cursor()
    for r in records:
        json_record = json.dumps(
            {
                "safe_gas_price": r.get("SafeGasPrice"),
                "propose_gas_price": r.get("ProposeGasPrice"),
                "fast_gas_price": r.get("FastGasPrice"),
            }
        )
        ingestion_time = datetime.now(timezone.utc)
        cs.execute(insert_stmt, (json_record, ingestion_time))
    cs.close()
    conn.close()
    logging.info(f"Inserted {len(records)} records into Snowflake.")


async def fetch_gas():
    await etherscan_limiter.wait()

    ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "")
    if not ETHERSCAN_API_KEY:
        logger.warning(
            "No Etherscan API key found. Using API without key may result in rate limiting."
        )
    url = f"https://api.etherscan.io/api?module=gastracker&action=gasoracle&apikey={ETHERSCAN_API_KEY}"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            r = await response.json()
    return r.get("result", {})


def run_async_fetch_and_load_gas_prices(**ctx):
    async def run():
        return await fetch_loop(fetch_gas, [()])

    records = asyncio.run(run())
    load_gas_prices_data(records)


def create_table_if_not_exists(schema_name: str, table_name: str, ddl: str):
    @task(task_id=f'create_{table_name}_table_if_not_exists')
    def _inner():
        conn = snowflake.connector.connect(
            **Variable.get("snowflake_conn_json", deserialize_json=True)
        )
        cs = conn.cursor()
        cs.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        cs.execute(f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} {ddl}")
        cs.close()
        conn.close()
    return _inner


@dag(
    dag_id='blockchain_data_fetch_and_load_dag',
    default_args=default_args,
    schedule='*/15 * * * *',
    start_date=pendulum.now('UTC').subtract(days=1),
    catchup=False,
    max_active_runs=1,
    tags=['blockchain', 'snowflake']
)
def blockchain_data_fetch_and_load_dag():
    @task(task_id='fetch_and_load_usdt_weth')
    def fetch_and_load_usdt_weth():
        run_async_fetch_and_load_usdt_weth()

    @task(task_id='fetch_and_load_gas_prices')
    def fetch_and_load_gas_prices():
        run_async_fetch_and_load_gas_prices()

    create_token_transfers_table_if_not_exists = create_table_if_not_exists(
        "raw", "token_transfers", "(payload VARIANT, ingestion_ts TIMESTAMP_NTZ)"
    )

    create_gas_prices_table_if_not_exists = create_table_if_not_exists(
        "raw", "gas_prices", "(payload VARIANT, ingestion_ts TIMESTAMP_NTZ)"
    )

    create_token_transfers_table_if_not_exists() >> fetch_and_load_usdt_weth() # pyright: ignore[reportUnusedExpression]
    create_gas_prices_table_if_not_exists() >> fetch_and_load_gas_prices() # pyright: ignore[reportUnusedExpression]

dag = blockchain_data_fetch_and_load_dag()

