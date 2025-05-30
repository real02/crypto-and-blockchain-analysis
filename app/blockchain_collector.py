import asyncio
import aiohttp
import logging
import time
import os
import certifi
import ssl

from aiohttp import ClientTimeout
from datetime import datetime
from dotenv import load_dotenv

from utils import async_retry

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("blockchain-collector")


def create_blockchain_record(data_type, specific_data):
    """
    Create a complete blockchain record with all possible fields initialized as None,
    then populate only the specific fields for this data_type

    Args:
        data_type: Type of blockchain data (gas_prices, latest_block, etc)
        specific_data: Dictionary containing the data for this type

    Returns:
        Complete record with all schema fields
    """
    # Create base record with common fields
    record = {
        "timestamp": datetime.now().isoformat(),
        "data_type": data_type,
        # Initialize all possible fields with None
        # Gas prices fields
        "safe_gas_price": None,
        "propose_gas_price": None,
        "fast_gas_price": None,
        "last_block": None,
        "suggested_base_fee": None,
        # Latest block fields
        "block_number": None,
        "block_time": None,
        "gas_used": None,
        "gas_limit": None,
        "transaction_count": None,
        "base_fee_per_gas": None,
        # Recent transaction fields
        "tx_hash": None,
        "from_address": None,
        "to_address": None,
        "gas_price_gwei": None,
        "gas_limit": None,
        "value_eth": None,
        "tx_timestamp": None,
        "tx_age_seconds": None,
        "function_name": None,
        # Network metrics fields
        "gas_usage_percentage": None,
        "block_fullness": None,
        "network_congestion": None,
        # Transaction statistics fields
        "avg_gas_price": None,
        "tx_count": None,
        "avg_tx_age_seconds": None,
    }

    # Update with the specific data for this type
    record.update(specific_data)
    return record


# Etherscan API rate limiter (5 requests per second maximum)
class EtherscanRateLimiter:
    def __init__(self, calls_per_second=5):
        self.calls_per_second = calls_per_second
        self.min_interval = 1.0 / calls_per_second
        self.last_call_time = 0
        self.lock = asyncio.Lock()

    async def wait(self):
        """Wait if needed to respect Etherscan rate limits"""
        async with self.lock:
            current_time = time.time()
            elapsed = current_time - self.last_call_time

            if elapsed < self.min_interval:
                wait_time = self.min_interval - elapsed
                logger.debug(f"Rate limiting Etherscan API: waiting {wait_time:.2f}s")
                await asyncio.sleep(wait_time)

            self.last_call_time = time.time()


# Create a global rate limiter instance
etherscan_limiter = EtherscanRateLimiter()


async def fetch_etherscan_data():
    """
    Collect blockchain data from Etherscan API endpoints with rate limiting
    """
    results = []
    collection_time = datetime.now().isoformat()

    # Get Etherscan API key from environment
    ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "")
    if not ETHERSCAN_API_KEY:
        logger.warning(
            "No Etherscan API key found. Using API without key may result in rate limiting."
        )

    ssl_context = ssl.create_default_context(cafile=certifi.where())

    # Function to fetch gas prices with rate limiting
    async def fetch_gas_prices():
        # Wait for rate limit
        await etherscan_limiter.wait()

        url = f"https://api.etherscan.io/v2/api?chainid=1&module=gastracker&action=gasoracle&apikey={ETHERSCAN_API_KEY}"
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url, timeout=ClientTimeout(total=15), ssl=ssl_context
            ) as response:
                return await response.json()

    # Function to fetch latest block number with rate limiting
    async def fetch_latest_block_number():
        # Wait for rate limit
        await etherscan_limiter.wait()

        url = f"https://api.etherscan.io/v2/api?chainid=1&module=proxy&action=eth_blockNumber&apikey={ETHERSCAN_API_KEY}"
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url, timeout=ClientTimeout(total=15), ssl=ssl_context
            ) as response:
                return await response.json()

    # Function to fetch block details by number with rate limiting
    async def fetch_block_details(block_number_hex):
        # Wait for rate limit
        await etherscan_limiter.wait()

        url = f"https://api.etherscan.io/v2/api?chainid=1&module=proxy&action=eth_getBlockByNumber&tag={block_number_hex}&boolean=true&apikey={ETHERSCAN_API_KEY}"
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url, timeout=ClientTimeout(total=15), ssl=ssl_context
            ) as response:
                return await response.json()

    # Function to fetch recent transactions with rate limiting
    async def fetch_eth_transfers():
        # Wait for rate limit
        await etherscan_limiter.wait()

        # Track ETH transfers which are more relevant to overall network activity
        # This is ETH 2.0 deposit address
        address = "0x00000000219ab540356cBB839Cbe05303d7705Fa"
        url = f"https://api.etherscan.io/v2/api?chainid=1&module=account&action=txlist&address={address}&page=1&offset=20&sort=desc&apikey={ETHERSCAN_API_KEY}"
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url, timeout=ClientTimeout(total=15), ssl=ssl_context
            ) as response:
                return await response.json()

    # TODO
    # to-be-used-later-in-the-code
    async def fetch_usdt_transactions():
        """
        Large USDT movements often precede significant market movements, which can affect arbitrage opportunities.
        """
        await etherscan_limiter.wait()

        # USDT contract address
        usdt_address = "0xdAC17F958D2ee523a2206206994597C13D831ec7"
        url = f"https://api.etherscan.io/v2/api?chainid=1&module=account&action=tokentx&address={usdt_address}&page=1&offset=20&sort=desc&apikey={ETHERSCAN_API_KEY}"
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url, timeout=ClientTimeout(total=15), ssl=ssl_context
            ) as response:
                return await response.json()

    # TODO
    # to-be-used-later-in-the-code
    async def fetch_weth_transactions():
        """Fetch WETH token transfer transactions.
        WETH (Wrapped Ether) is the ERC-20 token version of ETH used in many trading pairs.
        Measuring this activity could help to distinguish between ETH used for trading and ETH used for gas, giving cleaner signals about market activity.
        Also, traders often convert ETH to WETH when moving between exchanges and DeFi platforms.
        """
        await etherscan_limiter.wait()

        # WETH contract address on Ethereum
        weth_address = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"

        # Use the token transactions API
        url = f"https://api.etherscan.io/v2/api?chainid=1&module=account&action=tokentx&contractaddress={weth_address}&page=1&offset=20&sort=desc&apikey={ETHERSCAN_API_KEY}"

        async with aiohttp.ClientSession() as session:
            async with session.get(
                url, timeout=ClientTimeout(total=15), ssl=ssl_context
            ) as response:
                return await response.json()

    try:
        # 1. Fetch gas prices with retries
        try:
            gas_response = await async_retry(
                fetch_gas_prices,
                retries=3,
                backoff_factor=2,
                initial_wait=1,
                exceptions=(aiohttp.ClientError, asyncio.TimeoutError),
                func_name="fetch_gas_prices",
            )

            if gas_response.get("status") == "1" and gas_response.get("result"):
                gas_data = gas_response["result"]
                specific_data = {
                    "safe_gas_price": float(gas_data.get("SafeGasPrice", 0)),
                    "propose_gas_price": float(gas_data.get("ProposeGasPrice", 0)),
                    "fast_gas_price": float(gas_data.get("FastGasPrice", 0)),
                    "last_block": int(gas_data.get("LastBlock", 0)),
                    "suggested_base_fee": float(gas_data.get("suggestBaseFee", 0)),
                }
                record = create_blockchain_record("gas_prices", specific_data)
                results.append(record)
                logger.info(f"Gas prices: {record['fast_gas_price']} Gwei (fast)")
            else:
                logger.warning(
                    f"Failed to get gas prices: {gas_response.get('message', 'Unknown error')}"
                )

        except Exception as e:
            logger.error(f"Error fetching gas prices: {e}")

        # 2. Fetch latest block number and details
        try:
            # Get latest block number
            block_number_response = await async_retry(
                fetch_latest_block_number,
                retries=3,
                backoff_factor=2,
                initial_wait=1,
                exceptions=(aiohttp.ClientError, asyncio.TimeoutError),
                func_name="fetch_latest_block_number",
            )

            if block_number_response.get("result"):
                latest_block_hex = block_number_response["result"]
                latest_block_number = int(latest_block_hex, 16)
                logger.info(f"Latest block number: {latest_block_number}")

                # Get block details
                block_details_response = await async_retry(
                    lambda: fetch_block_details(latest_block_hex),
                    retries=3,
                    backoff_factor=2,
                    initial_wait=1,
                    exceptions=(aiohttp.ClientError, asyncio.TimeoutError),
                    func_name="fetch_block_details",
                )

                if block_details_response.get("result"):
                    block_data = block_details_response["result"]
                    specific_data = {
                        "block_number": latest_block_number,
                        "block_time": int(block_data.get("timestamp", "0x0"), 16),
                        "gas_used": int(block_data.get("gasUsed", "0x0"), 16),
                        "gas_limit": int(block_data.get("gasLimit", "0x0"), 16),
                        "transaction_count": len(block_data.get("transactions", [])),
                        "base_fee_per_gas": int(
                            block_data.get("baseFeePerGas", "0x0"), 16
                        )
                        / 1e9
                        if "baseFeePerGas" in block_data
                        else None,
                    }
                    record = create_blockchain_record("latest_block", specific_data)
                    results.append(record)
                    logger.info(
                        f"Block {latest_block_number} has {record['transaction_count']} transactions"
                    )
                else:
                    logger.warning(
                        f"Failed to get block details: {block_details_response.get('error', 'Unknown error')}"
                    )
            else:
                logger.warning(
                    f"Failed to get latest block number: {block_number_response.get('error', 'Unknown error')}"
                )

        except Exception as e:
            logger.error(f"Error fetching latest block: {e}")

        # 3. Fetch ETH transfers
        try:
            tx_response = await async_retry(
                fetch_eth_transfers,
                retries=3,
                backoff_factor=2,
                initial_wait=1,
                exceptions=(aiohttp.ClientError, asyncio.TimeoutError),
                func_name="fetch_eth_transfers",
            )

            if tx_response.get("status") == "1" and tx_response.get("result"):
                transactions = tx_response["result"]
                recent_tx_count = min(10, len(transactions))

                for tx in transactions[:recent_tx_count]:
                    # Convert string values to appropriate types
                    try:
                        gas_price = int(tx.get("gasPrice", 0)) / 1e9  # Wei to Gwei
                        gas_limit = int(tx.get("gas", 0))
                        value_eth = int(tx.get("value", 0)) / 1e18  # Wei to ETH
                        tx_timestamp = int(tx.get("timeStamp", 0))

                        specific_data = {
                            "tx_hash": tx.get("hash"),
                            "from_address": tx.get("from"),
                            "to_address": tx.get("to"),
                            "gas_price_gwei": gas_price,
                            "gas_limit": gas_limit,
                            "value_eth": value_eth,
                            "block_number": int(tx.get("blockNumber", 0)),
                            "tx_timestamp": tx_timestamp,
                            "tx_age_seconds": int(time.time()) - tx_timestamp,
                            "function_name": tx.get("functionName", ""),
                        }
                        record = create_blockchain_record(
                            "recent_transaction", specific_data
                        )
                        results.append(record)
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Error processing transaction data: {e}")

                logger.info(f"Collected {recent_tx_count} recent transactions")
            else:
                logger.warning(
                    f"Failed to get recent transactions: {tx_response.get('message', 'Unknown error')}"
                )

        except Exception as e:
            logger.error(f"Error fetching recent transactions: {e}")

        # 4. Calculate derived metrics
        if results:
            try:
                # Get the gas price data
                gas_data = next(
                    (item for item in results if item["data_type"] == "gas_prices"),
                    None,
                )
                # Get the block data
                block_data = next(
                    (item for item in results if item["data_type"] == "latest_block"),
                    None,
                )

                if gas_data and block_data:
                    # Calculate network congestion metrics
                    specific_data = {
                        "gas_usage_percentage": (
                            block_data["gas_used"] / block_data["gas_limit"]
                        )
                        * 100
                        if block_data["gas_limit"] > 0
                        else 0,
                        "fast_gas_price": gas_data["fast_gas_price"],
                        "block_fullness": "High"
                        if block_data["gas_used"] / block_data["gas_limit"] > 0.8
                        else "Medium"
                        if block_data["gas_used"] / block_data["gas_limit"] > 0.5
                        else "Low",
                        "network_congestion": "High"
                        if gas_data["fast_gas_price"] > 100
                        else "Medium"
                        if gas_data["fast_gas_price"] > 50
                        else "Low",
                    }
                    record = create_blockchain_record("network_metrics", specific_data)
                    results.append(record)
                    logger.info(
                        f"Network congestion: {record['network_congestion']}, Block fullness: {record['block_fullness']}"
                    )

                # Calculate average gas price from recent transactions
                recent_txs = [
                    item
                    for item in results
                    if item["data_type"] == "recent_transaction"
                ]
                if recent_txs:
                    avg_gas_price = sum(
                        tx["gas_price_gwei"] for tx in recent_txs
                    ) / len(recent_txs)

                    specific_data = {
                        "avg_gas_price": avg_gas_price,
                        "tx_count": len(recent_txs),
                        "avg_tx_age_seconds": sum(
                            tx.get("tx_age_seconds", 0) for tx in recent_txs
                        )
                        / len(recent_txs),
                    }

                    record = create_blockchain_record("tx_statistics", specific_data)
                    results.append(record)
                    logger.info(
                        f"Average gas price from recent transactions: {avg_gas_price:.2f} Gwei"
                    )

            except Exception as e:
                logger.error(f"Error calculating derived metrics: {e}")

    except Exception as e:
        logger.error(f"Error in fetch_etherscan_data: {e}")

    return results


async def main():
    """Test function to run the collector directly"""
    data = await fetch_etherscan_data()
    print(f"Collected {len(data)} blockchain data points")

    # Print each data type
    data_types = {}
    for item in data:
        data_type = item["data_type"]
        if data_type not in data_types:
            data_types[data_type] = 0
        data_types[data_type] += 1

    for data_type, count in data_types.items():
        print(f" - {data_type}: {count} records")


if __name__ == "__main__":
    asyncio.run(main())
