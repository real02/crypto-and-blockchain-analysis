import aiohttp
import asyncio
import logging
import os
import pandas as pd
import pendulum
import snowflake.connector

from dotenv import load_dotenv
from pathlib import Path

from airflow.decorators import dag, task
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

        rows = []

        for token, addr in [("usdt", USDT_ADDRESS), ("weth", WETH_ADDRESS)]:
            for wallet in CEX_ADDRESSES:
                txs = await fetch_all_token_transfers(
                    addr, wallet, start_block, end_block
                )
                for tx in txs:
                    rows.append(
                        {
                            "token": token,
                            "tx_hash": tx["hash"],
                            "from_addr": tx["from"].lower(),
                            "to_addr": tx["to"].lower(),
                            "value": tx["value"],
                            "time_stamp": tx["timeStamp"],
                            "block_number": tx["blockNumber"],
                        }
                    )

        conn = snowflake.connector.connect(**SNOWFLAKE_CONN, schema="RAW")
        cs = conn.cursor()
        stmt = """
            INSERT INTO usdt_weth_transfers_raw
              (token, tx_hash, from_addr, to_addr, value, time_stamp, block_number)
            SELECT %s, %s, %s, %s, %s, %s, %s
        """

        for row in rows:
            cs.execute(
                stmt,
                (
                    row["token"],
                    row["tx_hash"],
                    row["from_addr"],
                    row["to_addr"],
                    row["value"],
                    int(row["time_stamp"]),
                    int(row["block_number"]),
                ),
            )
        cs.close()
        conn.close()
        assert rows, "No transfer rows fetched"

    asyncio.run(run())


def run_async_fetch_and_load_gas_prices(**ctx):
    async def run():
        ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "")
        if not ETHERSCAN_API_KEY:
            logger.warning(
                "No Etherscan API key found. Using API without key may result in rate limiting."
            )

        url = f"https://api.etherscan.io/api?module=gastracker&action=gasoracle&apikey={ETHERSCAN_API_KEY}"

        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                r = await response.json()

        result = r.get("result", {})

        stmt = """
            INSERT INTO gas_prices_raw (safe_gas, propose_gas, fast_gas, timestamp)
            SELECT %s, %s, %s, current_timestamp()
        """

        conn = snowflake.connector.connect(**SNOWFLAKE_CONN, schema="RAW")
        cs = conn.cursor()
        cs.execute(
            stmt,
            (
                result.get("SafeGasPrice"),
                result.get("ProposeGasPrice"),
                result.get("FastGasPrice"),
            ),
        )
        cs.close()
        conn.close()
        assert "SafeGasPrice" in result

    asyncio.run(run())


@dag(
    dag_id='blockchain_data_fetch_and_load_dag',
    default_args=default_args,
    schedule='*/30 * * * *',
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

    fetch_and_load_usdt_weth() >> fetch_and_load_gas_prices() # pyright: ignore[reportUnusedExpression]

dag = blockchain_data_fetch_and_load_dag()

