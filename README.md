# Crypto Arbitrage Detection System

## Project Overview

This project implements a real-time crypto arbitrage detection system that monitors price differences across multiple exchanges and blockchain network conditions to identify profitable trading opportunities. By collecting high-frequency market data and blockchain metrics, the system can detect temporary price discrepancies while accounting for gas costs and network congestion.

### Primary Goals

- **Real-time Market Monitoring**: Track cryptocurrency prices across multiple exchanges simultaneously
- **Blockchain Network Analysis**: Monitor Ethereum gas prices, block fullness, and network congestion  
- **Arbitrage Detection**: Identify profitable trading opportunities with consideration for transaction costs
- **Decision Support**: Provide traders with actionable insights including risk assessment based on network conditions

## System Architecture

The architecture follows a modern event-driven approach with these key components:

### Components

1. **Data Collection Layer**
   - Python-based FastAPI service that collects data from:
     - Multiple cryptocurrency exchanges via CCXT library
     - Ethereum blockchain via Etherscan API
   - Optimized for parallel processing with rate limiting to respect API constraints

2. **Data Stream Processing**
   - Confluent Kafka message broker deployed on AWS EC2
   - Separate topics for exchange data and blockchain metrics

3. **Operational Database**
   - SingleStore (formerly MemSQL) for high-performance storage and analysis
   - Real-time data ingestion via Kafka pipelines
   - SQL stored procedures for continuous analysis and arbitrage detection

4. **Visualization**
   - Grafana dashboards for real-time monitoring and alerts
   - Custom refresh rates for high-frequency market data

## Technology Choices & Justification

### Data Collection

- **FastAPI**: High-performance async framework ideal for handling concurrent API requests with minimal latency
- **CCXT**: Unified interface to access 100+ cryptocurrency exchanges with consistent data structures
- **Async I/O & Rate Limiting**: Implementation of custom rate limiters ensures adherence to exchange-specific API limits while maximizing throughput

### Data Streaming

- **Confluent Kafka**: Enterprise-grade message broker that provides:
  - High throughput for thousands of market data points per second
  - Persistent storage of data streams
  - Fault tolerance with replication
  - Schema registry capabilities (though not fully implemented in this version)

### Operational Database

- **SingleStore**: Chosen for its unique capabilities as a unified database that offers:
  - Fast data ingestion from Kafka streams
  - SQL pipelines for continuous ETL processes
  - Sub-millisecond query performance for both OLTP and OLAP workloads
  - Support for both row and column-oriented storage
  - JSON support for flexible handling of exchange-specific data structures

### Deployment

- **AWS EC2**: Provides the flexibility and control needed for deploying the FastAPI service and configuring the required refresh rates for real-time monitoring

## Data Model

The system uses a structured data model with these primary entities:

### Exchange Data
- Pricing information from multiple exchanges
- Order book depth (top bids/asks)
- Spread metrics

### Blockchain Data
- Gas prices (fast, safe, proposed)
- Block statistics (fullness, transaction count)
- Recent transaction metrics
- Network congestion indicators

### Derived Data
- Arbitrage opportunities
- Network status summaries
- Exchange price comparisons
- Transaction statistics history

## Implementation Details

### Data Collection

The system uses a parallel processing approach to efficiently collect data from multiple sources:

1. **Exchange Data Collection**:
   - Concurrent requests to different exchanges
   - Sequential processing within each exchange to respect rate limits
   - Collection of both ticker data and order book depth

2. **Blockchain Data Collection**:
   - Asynchronous retrieval of Ethereum network metrics
   - Exponential backoff retry mechanism for handling API failures
   - Derived metrics calculation including network congestion indicators

### Processing Pipeline

1. **Data Ingestion**:
   - FastAPI endpoints trigger data collection background tasks
   - Configurable continuous collection mode with adaptive interval timing
   - Data sent to Kafka topics with compression for efficiency

2. **Storage and Analysis**:
   - SingleStore pipelines consume Kafka topics in real-time
   - Stored procedures run on schedule to:
     - Refresh network status views
     - Calculate price comparisons across exchanges
     - Detect arbitrage opportunities based on configurable thresholds
     - Incorporate gas costs and network conditions into profitability calculations

### Scheduling

- Cron jobs execute stored procedures at appropriate intervals:
  - Network status updates every 30 seconds
  - Price comparison refreshes every 10 seconds
  - Arbitrage detection runs every 10 seconds
  - Transaction statistics aggregated every 15 minutes

## Challenges and Solutions

### Exchange API Rate Limiting

**Challenge**: Cryptocurrency exchanges enforce strict rate limits that vary widely between providers.

**Solution**: Implemented a custom rate limiting system with:
- Per-exchange async rate limiters
- Parallel processing across exchanges but sequential within each exchange
- Configurable calls-per-second parameters

### Volatile Gas Prices

**Challenge**: Ethereum gas prices can fluctuate rapidly, affecting the profitability of arbitrage opportunities.

**Solution**:
- Real-time monitoring of gas prices via Etherscan API
- Incorporation of gas costs into arbitrage profit calculations
- Risk categorization based on network congestion

### Data Synchronization

**Challenge**: Ensuring that exchange data and blockchain metrics are time-synchronized for accurate analysis.

**Solution**:
- Timestamp standardization across all data sources
- Collection of data in parallel to minimize time differences
- Time-window based analysis in stored procedures

## Performance Metrics

The system achieves:

- Data collection cycle times of ~1-3 seconds (depending on configured exchanges)
- Sub-second query response times for arbitrage detection
- End-to-end latency from market change to dashboard update of ~5 seconds
- Capacity to monitor dozens of trading pairs across multiple exchanges simultaneously

## Future Enhancements

Several potential improvements could extend the system's capabilities:

1. **Advanced Analytics**:
   - Machine learning models to predict profitable arbitrage windows
   - Pattern recognition for recurring market inefficiencies
   - Time-series analysis for understanding market dynamics

2. **Additional Data Sources**:
   - Integration with decentralized exchanges (DEXs)
   - Social media sentiment analysis
   - Order book imbalance metrics

3. **Automated Trading**:
   - Trading strategy execution through exchange APIs
   - Risk management and position sizing algorithms
   - Performance tracking and strategy backtesting

4. **System Improvements**:
   - Schema registry implementation for Kafka topics
   - Horizontal scaling of the data collection service
   - Enhanced security features (API key rotation, etc.)
   - Fault tolerance improvements with circuit breakers

## Getting Started

### Prerequisites

- Python 3.8+
- Confluent Kafka account
- SingleStore account
- Etherscan API key
- Exchange API keys as needed

### Configuration

1. Create a `.env` file with the following environment variables:
   ```
   ETHERSCAN_API_KEY=your_key_here
   CONFLUENT_BOOTSTRAP_SERVERS=your_servers_here
   CONFLUENT_API_KEY=your_key_here
   CONFLUENT_API_SECRET=your_secret_here
   EXCHANGE_TOPIC=exchange-data
   BLOCKCHAIN_TOPIC=blockchain-data
   SINGLESTORE_HOST=your_host_here
   SINGLESTORE_PORT=3306
   SINGLESTORE_USER=your_user_here
   SINGLESTORE_PASSWORD=your_password_here
   SINGLESTORE_DB=crypto_arbitrage
   ```

2. Update SingleStore pipeline configurations in `processing_and_analysis.sql`

### Deployment

1. Start the FastAPI service:
   ```
   uvicorn fastapi_listener:app --host 0.0.0.0 --port 8000
   ```

2. Set up the cron jobs for database procedures:
   ```
   ./setup_crontab.sh
   ```

3. Configure Grafana to connect to SingleStore and create dashboards

## Conclusion

This crypto arbitrage detection system demonstrates a modern approach to real-time financial data analysis. By combining fast data collection, stream processing, and high-performance database technology, it provides traders with actionable insights into market inefficiencies that would otherwise be impossible to detect manually.

The architecture prioritizes performance, scalability, and reliability—key requirements for any system operating in the fast-paced cryptocurrency markets.
