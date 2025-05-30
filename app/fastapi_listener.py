from fastapi import FastAPI, BackgroundTasks, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware

from typing import Dict, List
import asyncio
import time
import logging
import ccxt
import json
import os
import uvicorn
import certifi
import ssl

from datetime import datetime
from kafka import KafkaProducer
from pydantic import BaseModel
from dotenv import load_dotenv

# Import our blockchain data collector
from blockchain_collector import fetch_etherscan_data
from utils import async_retry

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("arbitrage-service")

# Initialize FastAPI app
app = FastAPI(title="Crypto Arbitrage Data Service")

# Kafka configuration for Confluent Cloud
CONFLUENT_BOOTSTRAP_SERVERS = os.getenv("CONFLUENT_BOOTSTRAP_SERVERS", "")
CONFLUENT_API_KEY = os.getenv("CONFLUENT_API_KEY", "")
CONFLUENT_API_SECRET = os.getenv("CONFLUENT_API_SECRET", "")
EXCHANGE_TOPIC = os.getenv("EXCHANGE_TOPIC", "exchange-data")
BLOCKCHAIN_TOPIC = os.getenv("BLOCKCHAIN_TOPIC", "blockchain-data")

# Initialize Kafka producer with Confluent Cloud config
try:
    producer = KafkaProducer(
        bootstrap_servers=CONFLUENT_BOOTSTRAP_SERVERS,
        security_protocol="SASL_SSL",
        sasl_mechanism="PLAIN",
        sasl_plain_username=CONFLUENT_API_KEY,
        sasl_plain_password=CONFLUENT_API_SECRET,
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        compression_type="gzip",
        ssl_cafile=certifi.where(),
    )
    logger.info("Kafka producer initialized successfully with Confluent Cloud")
except Exception as e:
    logger.error(f"Failed to initialize Kafka producer: {e}")
    producer = None


# Input model for collection request
class CollectionRequest(BaseModel):
    exchanges: List[str] = ["binance", "kraken"]
    symbols: List[str] = [
        "BTC/USDT",
        "ETH/USDT",
        "SOL/USDT",
        "ADA/USDT",
        "DOT/USDT",
    ]
    continuous_mode: bool = True
    interval_seconds: float = 5.0


# Track running collection tasks and rate limiters
active_collections = {}
exchange_rate_limiters: Dict[str, "ExchangeRateLimiter"] = {}


class ExchangeRateLimiter:
    """Rate limiter for exchange API calls"""

    def __init__(self, exchange_id, calls_per_second=1):
        self.exchange_id = exchange_id
        self.min_interval = (
            1.0 / calls_per_second
        )  # Minimum interval between calls (in seconds)
        self.last_call_time = 0
        self.lock = asyncio.Lock()

    async def wait(self):
        """Wait if needed to respect rate limits"""
        async with self.lock:
            current_time = time.time()
            elapsed = current_time - self.last_call_time

            if elapsed < self.min_interval:
                wait_time = self.min_interval - elapsed
                logger.debug(
                    f"Rate limiting {self.exchange_id}: waiting {wait_time:.2f}s"
                )
                await asyncio.sleep(wait_time)

            self.last_call_time = time.time()


async def rate_limited_fetch_exchange_data(exchange_id, symbols):
    """
    Fetch data for a specific exchange with rate limiting

    This function processes symbols sequentially for a single exchange to respect rate limits
    """
    try:
        # Get or create rate limiter for this exchange
        if exchange_id not in exchange_rate_limiters:
            exchange_rate_limiters[exchange_id] = ExchangeRateLimiter(exchange_id)

        rate_limiter = exchange_rate_limiters[exchange_id]

        # Initialize exchange
        exchange_class = getattr(ccxt, exchange_id)
        exchange = exchange_class(
            {
                "enableRateLimit": True,  # Enable CCXT's built-in rate limiting
                "verify": False,  # In production, set to True to verify SSL
            }
        )

        results = []
        collection_time = datetime.now().isoformat()

        try:
            # Wait for rate limit before fetching ticker
            await rate_limiter.wait()
            tickers = exchange.fetch_tickers(symbols)

            # Wait for rate limit before fetching order book
            await rate_limiter.wait()

            # total_orderbook = {}
            # for symbol in symbols:
            #     order_book = exchange.fetch_order_book(symbol, limit=5)
            #     total_orderbook[symbol] = order_book

            # Create record with all the data
            for symbol, ticker in tickers.items():
                # order_book = total_orderbook.get(symbol, {"bids": [], "asks": []})
                # record = {
                #     "timestamp": collection_time,
                #     "exchange": exchange_id,
                #     "symbol": symbol,
                #     "bid": ticker["bid"],
                #     "ask": ticker["ask"],
                #     "last": ticker["last"],
                #     "bid_volume": ticker.get("bidVolume", None),
                #     "ask_volume": ticker.get("askVolume", None),
                #     # "top_bids": order_book["bids"][:5] if order_book["bids"] else [],
                #     # "top_asks": order_book["asks"][:5] if order_book["asks"] else [],
                #     "spread": ticker["ask"] - ticker["bid"]
                #     if ticker["ask"] and ticker["bid"]
                #     else None,
                #     "spread_percentage": (
                #         (ticker["ask"] - ticker["bid"]) / ticker["bid"] * 100
                #     )
                #     if ticker["ask"] and ticker["bid"]
                #     else None,
                # }
                record = {
                    "timestamp": collection_time,
                    "exchange": exchange_id,
                    "symbol": symbol,
                    "bid": ticker.get("bid", 0.0),
                    "ask": ticker.get("ask", 0.0),
                    "last": ticker.get("last", 0.0),
                    "bid_volume": ticker.get("bidVolume", 0.0),
                    "ask_volume": ticker.get("askVolume", 0.0),
                }
                bid = ticker.get("bid")
                ask = ticker.get("ask")
                if bid is not None and ask is not None and bid > 0:
                    record["spread"] = ask - bid
                    record["spread_percentage"] = ((ask - bid) / bid) * 100
                else:
                    record["spread"] = 0.0
                    record["spread_percentage"] = 0.0
                results.append(record)
            logger.debug(f"Successfully fetched {symbols} from {exchange_id}")

        except Exception as e:
            logger.warning(
                f"Error fetching {symbols} from {exchange_id}: {e}", exc_info=True
            )

        return results

    except Exception as e:
        logger.error(f"Error processing exchange {exchange_id}: {e}")
        return []


async def collect_all_data(
    task_id, exchanges, symbols, is_continuous=True, interval=5.0
):
    """
    Collect all data in parallel while respecting rate limits

    - Different exchanges are processed in parallel
    - Within each exchange, symbols are processed sequentially with rate limiting
    - Blockchain data is collected in parallel with exchange data
    """
    logger.info(f"Starting data collection task {task_id}")

    running = True
    collection_count = 0

    # Update task status
    active_collections[task_id] = {
        "id": task_id,
        "start_time": datetime.now().isoformat(),
        "status": "running",
        "exchanges": exchanges,
        "symbols": symbols,
        "continuous": is_continuous,
        "interval": interval,
        "total_collected": 0,
        "cycles_completed": 0,
        "last_run": None,
    }

    while running:
        start_time = time.time()
        collection_time = datetime.now().isoformat()

        try:
            # Process different exchanges in parallel, but rate-limit within each exchange
            exchange_tasks = []
            for exchange in exchanges:
                # Create a closure to capture the exchange for the retry function
                async def fetch_exchange_data_for_retry(exchange_id=exchange):
                    return await rate_limited_fetch_exchange_data(exchange_id, symbols)

                task = async_retry(
                    fetch_exchange_data_for_retry,
                    retries=3,
                    backoff_factor=2,
                    initial_wait=1,
                    exceptions=(Exception,),
                    func_name=f"exchange:{exchange}",
                )
                exchange_tasks.append(task)

            # Add blockchain data collection task with retry
            blockchain_task = async_retry(
                fetch_etherscan_data,
                retries=3,
                backoff_factor=2,
                initial_wait=1,
                exceptions=(Exception,),
                func_name="fetch_etherscan_data",
            )

            # Run all tasks concurrently
            all_results = await asyncio.gather(
                *exchange_tasks, blockchain_task, return_exceptions=True
            )

            # print("FIRST DATA: ", all_results[:-1])

            # Process exchange data results (all but the last result, which is blockchain data)
            exchange_data = []
            failed_exchanges = []
            for idx, result in enumerate(all_results[:-1]):
                if isinstance(result, Exception):
                    logger.error(f"Error collecting from {exchanges[idx]}: {result}")
                    failed_exchanges.append(exchanges[idx])
                else:
                    if isinstance(result, list):
                        exchange_data.extend(result)
                    else:
                        logger.error(
                            f"Unexpected result type from {exchanges[idx]}: {type(result)}"
                        )
                        failed_exchanges.append(exchanges[idx])

            # print("ALL DATA: ", all_results)
            # print("LAST DATA: ", all_results[-1])

            # Process blockchain data result (the last result)
            blockchain_data = []
            blockchain_failed = False
            if isinstance(all_results[-1], Exception):
                logger.error(f"Error collecting blockchain data: {all_results[-1]}")
                blockchain_failed = True
            else:
                if isinstance(all_results[-1], list):
                    blockchain_data = all_results[-1]
                else:
                    logger.error(
                        f"Unexpected blockchain data type: {type(all_results[-1])}"
                    )
                    blockchain_failed = True

            # Log summary of collection
            logger.info(
                f"Collection summary: {len(exchange_data)} exchange records, {len(blockchain_data)} blockchain records"
            )
            if failed_exchanges:
                logger.warning(
                    f"Failed to collect from exchanges: {', '.join(failed_exchanges)}"
                )
            if blockchain_failed:
                logger.warning("Failed to collect blockchain data")

            # Send data to Kafka if producer is available
            if producer and (exchange_data or blockchain_data):
                # Send exchange data
                for item in exchange_data:
                    producer.send(EXCHANGE_TOPIC, value=item)

                # Send blockchain data
                for item in blockchain_data:
                    producer.send(BLOCKCHAIN_TOPIC, value=item)

                # Ensure all messages are sent
                producer.flush()

            # Update task status
            collection_count += 1
            total_collected = len(exchange_data) + len(blockchain_data)

            active_collections[task_id]["last_run"] = collection_time
            active_collections[task_id]["total_collected"] += total_collected
            active_collections[task_id]["cycles_completed"] = collection_count

            # Log collection statistics
            cycle_time = time.time() - start_time
            logger.info(
                f"Task {task_id}: Collected {len(exchange_data)} exchange records and {len(blockchain_data)} blockchain records in {cycle_time:.2f}s"
            )

            # If not continuous mode, break after one iteration
            if not is_continuous:
                running = False
                active_collections[task_id]["status"] = "completed"
                logger.info(f"Task {task_id} completed")
                break

            # Check if task was requested to stop
            if active_collections[task_id].get("status") == "stopping":
                running = False
                active_collections[task_id]["status"] = "stopped"
                logger.info(f"Task {task_id} stopped by request")
                break

            # Calculate sleep time to maintain the desired interval
            elapsed = time.time() - start_time
            sleep_time = max(0.1, interval - elapsed)
            if sleep_time > 0:
                logger.debug(f"Waiting {sleep_time:.2f}s until next collection cycle")
                await asyncio.sleep(sleep_time)

        except asyncio.CancelledError:
            logger.info(f"Task {task_id} was cancelled")
            active_collections[task_id]["status"] = "cancelled"
            break
        except Exception as e:
            logger.error(f"Error in data collection task {task_id}: {e}")
            active_collections[task_id]["status"] = "error"
            active_collections[task_id]["error"] = str(e)
            if not is_continuous:
                break
            # For continuous mode, wait a bit and try again
            await asyncio.sleep(5)


@app.post("/collect")
async def trigger_collection(
    background_tasks: BackgroundTasks, request: CollectionRequest = Body(...)
):
    """
    Trigger data collection with parallel processing and rate limiting.

    This endpoint optimizes:
    - Processing different exchanges in parallel
    - Rate-limiting requests within each exchange
    - Collecting blockchain data from Etherscan
    """
    # Validate exchanges
    available_exchanges = ["binance", "kraken", "bitfinex", "kucoin", "huobijp"]
    for exchange in request.exchanges:
        if exchange not in available_exchanges:
            raise HTTPException(
                status_code=400,
                detail=f"Exchange {exchange} is not supported. Available: {available_exchanges}",
            )

    # Generate a unique task ID
    task_id = f"collection-{int(time.time())}"

    # Start the background task
    background_tasks.add_task(
        collect_all_data,
        task_id,
        request.exchanges,
        request.symbols,
        request.continuous_mode,
        request.interval_seconds,
    )

    return {
        "task_id": task_id,
        "status": "started",
        "message": f"Started collection from {len(request.exchanges)} exchanges and Etherscan blockchain data",
        "continuous_mode": request.continuous_mode,
        "interval_seconds": request.interval_seconds
        if request.continuous_mode
        else None,
    }


@app.get("/stop/{task_id}")
async def stop_collection(task_id: str):
    """Stop a running continuous collection task"""
    if task_id not in active_collections:
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found")

    task_info = active_collections[task_id]

    if task_info["status"] != "running":
        return {
            "task_id": task_id,
            "status": task_info["status"],
            "message": f"Task is not running (current status: {task_info['status']})",
        }

    # Update status
    task_info["status"] = "stopping"

    return {
        "task_id": task_id,
        "status": "stopping",
        "message": f"Requested to stop task {task_id}",
    }


@app.get("/status")
async def service_status():
    """Get service status and active collection tasks"""
    kafka_status = "connected" if producer else "disconnected"

    # Count tasks by status
    task_count = {
        "running": len(
            [t for t in active_collections.values() if t["status"] == "running"]
        ),
        "completed": len(
            [t for t in active_collections.values() if t["status"] == "completed"]
        ),
        "error": len(
            [t for t in active_collections.values() if t["status"] == "error"]
        ),
        "total": len(active_collections),
    }

    return {
        "status": "operational" if producer else "degraded",
        "message": f"Service is operational. Kafka: {kafka_status}",
        "kafka_connected": producer is not None,
        "active_tasks": task_count,
        "collection_tasks": list(active_collections.values()),
    }


@app.get("/tasks")
async def list_tasks():
    """List all collection tasks"""
    return {"tasks": list(active_collections.values())}


@app.get("/exchanges")
async def get_available_exchanges():
    """Get list of available exchanges (useful for Grafana dropdowns)"""
    return ["binance", "kraken", "coinbase", "kucoin", "huobi"]


@app.get("/symbols")
async def get_default_symbols():
    """Get list of default symbols (useful for Grafana dropdowns)"""
    return ["BTC/USDT", "ETH/USDT", "SOL/USDT", "ADA/USDT", "DOT/USDT"]


@app.get("/")
async def root():
    """Service health check"""
    return {
        "status": "online",
        "service": "Crypto Arbitrage Data Service",
        "version": "1.0",
    }


# Add CORS middleware for Grafana integration

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, restrict this to your Grafana domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

if __name__ == "__main__":
    uvicorn.run("fastapi_listener:app", host="0.0.0.0", port=8000, reload=True)
