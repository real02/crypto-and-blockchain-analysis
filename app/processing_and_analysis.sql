-- Create database if it doesn't exist
CREATE DATABASE IF NOT EXISTS crypto_arbitrage;
USE crypto_arbitrage;

-- Exchange data table 
CREATE TABLE IF NOT EXISTS exchange_data (
    id BIGINT AUTO_INCREMENT,
    timestamp DATETIME NOT NULL,
    exchange VARCHAR(50) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    bid DOUBLE NULL,
    ask DOUBLE NULL,
    last DOUBLE NULL,
    bid_volume DOUBLE NULL,
    ask_volume DOUBLE NULL,
    spread DOUBLE NULL,
    spread_percentage DOUBLE NULL,

    PRIMARY KEY (id, exchange, symbol),
    KEY (timestamp, exchange, symbol),
    SHARD KEY (exchange, symbol)
);

-- Blockchain data table
CREATE TABLE IF NOT EXISTS blockchain_data (
    id BIGINT AUTO_INCREMENT,
    timestamp DATETIME NOT NULL,
    -- data_type ENUM('gas_prices', 'latest_block', 'recent_transaction', 'network_metrics', 'tx_statistics') NOT NULL,
    data_type VARCHAR(20) NOT NULL,
 
    -- Gas prices fields
    safe_gas_price DOUBLE NULL,
    propose_gas_price DOUBLE NULL,
    fast_gas_price DOUBLE NULL,
    last_block BIGINT NULL,
    suggested_base_fee DOUBLE NULL,
 
    -- Latest block fields
    block_number BIGINT NULL,
    block_time BIGINT NULL,
    gas_used BIGINT NULL,
    gas_limit BIGINT NULL,
    transaction_count INT NULL,
    base_fee_per_gas DOUBLE NULL,

    -- Recent transaction fields
    tx_hash VARCHAR(66) NULL,
    from_address VARCHAR(42) NULL,
    to_address VARCHAR(42) NULL,
    gas_price_gwei DOUBLE NULL,
    value_eth DOUBLE NULL,
    tx_timestamp BIGINT NULL,
    tx_age_seconds INT NULL,
    function_name TEXT NULL,

    -- Network metrics fields
    gas_usage_percentage DOUBLE NULL,
    block_fullness VARCHAR(10) NULL,
    network_congestion VARCHAR(10) NULL,

    -- Transaction statistics fields
    avg_gas_price DOUBLE NULL,
    tx_count INT NULL,
    avg_tx_age_seconds DOUBLE NULL,

    PRIMARY KEY (id, data_type),
    KEY (timestamp, data_type),
    SHARD KEY (data_type)
);

-- Arbitrage opportunities table
CREATE TABLE IF NOT EXISTS arbitrage_opportunities (
    id BIGINT AUTO_INCREMENT,
    detection_timestamp DATETIME NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    buy_exchange VARCHAR(50) NOT NULL,
    sell_exchange VARCHAR(50) NOT NULL,
    buy_price DOUBLE NOT NULL,
    sell_price DOUBLE NOT NULL,
    volume_constraint DOUBLE NOT NULL,  -- Min of available volumes
    gross_profit_usd DOUBLE NOT NULL,
    gross_profit_percentage DOUBLE NOT NULL,
    estimated_gas_cost_usd DOUBLE NOT NULL,
    net_profit_usd DOUBLE NOT NULL,
    net_profit_percentage DOUBLE NOT NULL,
    current_gas_gwei DOUBLE NOT NULL,
    risk_level VARCHAR(10) NOT NULL,  -- 'LOW', 'MEDIUM', 'HIGH'
 
    PRIMARY KEY (id, buy_exchange, sell_exchange),
    KEY (detection_timestamp, symbol),
    SHARD KEY (buy_exchange, sell_exchange)
);

-- Create tables for derived/aggregated data
-- These will act similar to materialized views by being refreshed periodically

-- Network status table
CREATE TABLE IF NOT EXISTS network_status (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    timestamp DATETIME NOT NULL,
    fast_gas_price DOUBLE NOT NULL,
    safe_gas_price DOUBLE NOT NULL,
    suggested_base_fee DOUBLE NULL,
    block_number BIGINT NOT NULL,
    transaction_count INT NOT NULL,
    gas_used BIGINT NOT NULL,
    gas_limit BIGINT NOT NULL,
    block_fullness_percent DOUBLE NOT NULL,
    network_congestion VARCHAR(10) NOT NULL,
    block_fullness VARCHAR(10) NOT NULL,
 
    KEY (timestamp)
);

-- Transaction statistics history
CREATE TABLE IF NOT EXISTS tx_stats_history (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    hour DATETIME NOT NULL,
    avg_gas_price DOUBLE NOT NULL,
    avg_tx_count DOUBLE NOT NULL,
    avg_confirmation_time DOUBLE NOT NULL,
 
    KEY (hour)
);

-- Price comparison table
CREATE TABLE IF NOT EXISTS price_comparison (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    timestamp DATETIME NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    exchange VARCHAR(50) NOT NULL,
    price DOUBLE NOT NULL,
    volume DOUBLE NULL,
    avg_other_exchanges DOUBLE NOT NULL,
    price_diff_percentage DOUBLE NOT NULL,
 
    KEY (timestamp, symbol, exchange)
);

-- Create regular views for querying
-- Note: These are not materialized, they're computed at query time

-- View for latest gas prices
CREATE OR REPLACE VIEW latest_gas_prices AS
SELECT * FROM blockchain_data
WHERE data_type = 'gas_prices'
ORDER BY timestamp DESC
LIMIT 1;

-- View for latest block
CREATE OR REPLACE VIEW latest_block AS
SELECT * FROM blockchain_data
WHERE data_type = 'latest_block'
ORDER BY timestamp DESC
LIMIT 1;

-- View for most recent transactions
CREATE OR REPLACE VIEW recent_transactions AS
SELECT * FROM blockchain_data
WHERE data_type = 'recent_transaction'
ORDER BY timestamp DESC
LIMIT 20;

-- View for current arbitrage opportunities
DROP VIEW IF EXISTS current_arbitrage_opportunities

CREATE VIEW current_arbitrage_opportunities AS
SELECT * FROM arbitrage_opportunities
WHERE detection_timestamp >= DATE_SUB(NOW(), INTERVAL 1 MINUTE)
ORDER BY net_profit_percentage DESC;

-- Create stored procedures to refresh the derived data tables

DELIMITER //

-- Procedure to refresh network status
CREATE OR REPLACE PROCEDURE refresh_network_status()
BEGIN
    -- Clear existing recent data (keep only entries older than 1 hour for history)
    DELETE FROM network_status 
    WHERE timestamp >= DATE_SUB(NOW(), INTERVAL 1 HOUR);
 
    -- Insert fresh data
    INSERT INTO network_status (
        timestamp,
        fast_gas_price,
        safe_gas_price,
        suggested_base_fee,
        block_number,
        transaction_count,
        gas_used,
        gas_limit,
        block_fullness_percent,
        network_congestion,
        block_fullness
    )
    SELECT 
        g.timestamp,
        g.fast_gas_price,
        g.safe_gas_price,
        g.suggested_base_fee,
        b.block_number,
        b.transaction_count,
        b.gas_used,
        b.gas_limit,
        (b.gas_used / b.gas_limit) * 100 AS block_fullness_percent,
        n.network_congestion,
        n.block_fullness
    FROM 
        blockchain_data g
    JOIN 
        blockchain_data b ON g.timestamp = b.timestamp 
                          AND g.data_type = 'gas_prices' 
                          AND b.data_type = 'latest_block'
    LEFT JOIN
        blockchain_data n ON g.timestamp = n.timestamp 
                          AND n.data_type = 'network_metrics'
    WHERE
        g.timestamp >= DATE_SUB(NOW(), INTERVAL 10 MINUTE)
    ORDER BY 
        g.timestamp DESC
    LIMIT 1;
END//

-- Procedure to refresh transaction statistics history
CREATE OR REPLACE PROCEDURE refresh_tx_stats_history()
BEGIN
    -- Clear existing recent data
    DELETE FROM tx_stats_history 
    WHERE hour >= DATE_SUB(NOW(), INTERVAL 1 DAY);
 
    -- Insert fresh hourly aggregated data
    INSERT INTO tx_stats_history (
        hour,
        avg_gas_price,
        avg_tx_count,
        avg_confirmation_time
    )
    SELECT 
        DATE_FORMAT(timestamp, '%Y-%m-%d %H:00:00') AS hour,
        AVG(avg_gas_price) AS avg_gas_price,
        AVG(tx_count) AS avg_tx_count,
        AVG(avg_tx_age_seconds) AS avg_confirmation_time
    FROM 
        blockchain_data
    WHERE 
        data_type = 'tx_statistics'
        AND timestamp >= DATE_SUB(NOW(), INTERVAL 1 DAY)
    GROUP BY 
        DATE_FORMAT(timestamp, '%Y-%m-%d %H:00:00')
    ORDER BY 
        hour;
END//

-- Procedure to refresh price comparison data
CREATE OR REPLACE PROCEDURE refresh_price_comparison()
BEGIN
    -- Clear existing recent data
    DELETE FROM price_comparison 
    WHERE timestamp >= DATE_SUB(NOW(), INTERVAL 5 MINUTE);
 
    -- Insert fresh data
    INSERT INTO price_comparison (
        timestamp,
        symbol,
        exchange,
        price,
        volume,
        avg_other_exchanges,
        price_diff_percentage
    )
    SELECT 
        e.timestamp,
        e.symbol,
        e.exchange,
        e.last AS price,
        e.volume,
        AVG(CASE WHEN e2.exchange != e.exchange THEN e2.last ELSE NULL END) AS avg_other_exchanges,
        (e.last - AVG(CASE WHEN e2.exchange != e.exchange THEN e2.last ELSE NULL END)) / 
            AVG(CASE WHEN e2.exchange != e.exchange THEN e2.last ELSE NULL END) * 100 AS price_diff_percentage
    FROM 
        exchange_data e
    JOIN 
        exchange_data e2 ON e.symbol = e2.symbol AND e.timestamp = e2.timestamp
    WHERE
        e.timestamp >= DATE_SUB(NOW(), INTERVAL 5 MINUTE)
    GROUP BY 
        e.timestamp, e.symbol, e.exchange, e.last, e.volume
    HAVING 
        COUNT(CASE WHEN e2.exchange != e.exchange THEN 1 ELSE NULL END) > 0;
END//

-- Procedure for detecting arbitrage opportunities
CREATE OR REPLACE PROCEDURE detect_arbitrage_opportunities(
    IN min_profit_percentage DOUBLE,
    IN check_minutes INT
)
BEGIN
    -- Get current gas price from blockchain data
    DECLARE current_gas DOUBLE;
 
    SELECT 
        fast_gas_price INTO current_gas 
    FROM 
        latest_gas_prices
    LIMIT 1;
 
    -- If no gas price is available, use a reasonable default
    IF current_gas IS NULL THEN
        SET current_gas = 50.0;  -- 50 Gwei as default
    END IF;
 
    -- Estimate gas cost in USD
    DECLARE gas_units BIGINT DEFAULT 150000;  -- Typical units for a DEX swap
    DECLARE eth_price DOUBLE;
 
    -- Get latest ETH price from exchange data
    SELECT 
        AVG(last) INTO eth_price
    FROM 
        exchange_data 
    WHERE 
        symbol = 'ETH/USDT' 
        AND timestamp >= DATE_SUB(NOW(), INTERVAL 10 MINUTE);
 
    -- Use default if not available
    IF eth_price IS NULL THEN
        SET eth_price = 3000.0;  -- Default ETH price
    END IF;
 
    -- Calculate gas cost in USD
    DECLARE gas_cost_usd DOUBLE;
    SET gas_cost_usd = (current_gas * gas_units * 0.000000001) * eth_price;
 
    -- Get network congestion level
    DECLARE congestion_level VARCHAR(10);
    SELECT 
        network_congestion INTO congestion_level
    FROM 
        network_status
    ORDER BY
        timestamp DESC
    LIMIT 1;
 
    -- Use default if not available
    IF congestion_level IS NULL THEN
        IF current_gas > 100 THEN
            SET congestion_level = 'HIGH';
        ELSEIF current_gas > 50 THEN
            SET congestion_level = 'MEDIUM';
        ELSE
            SET congestion_level = 'LOW';
        END IF;
    END IF;
 
    -- Find arbitrage opportunities
    INSERT INTO arbitrage_opportunities (
        detection_timestamp,
        symbol,
        buy_exchange,
        sell_exchange,
        buy_price,
        sell_price,
        volume_constraint,
        gross_profit_usd,
        gross_profit_percentage,
        estimated_gas_cost_usd,
        net_profit_usd,
        net_profit_percentage,
        current_gas_gwei,
        risk_level
    )
    SELECT 
        NOW() as detection_timestamp,
        e1.symbol,
        e1.exchange as buy_exchange,
        e2.exchange as sell_exchange,
        e1.ask as buy_price,
        e2.bid as sell_price,
        LEAST(e1.bid_volume, e2.ask_volume, 1.0) as volume_constraint,  -- Default to 1 unit if volumes are NULL
        (e2.bid - e1.ask) * LEAST(e1.bid_volume, e2.ask_volume, 1.0) as gross_profit_usd,
        (e2.bid - e1.ask) / e1.ask * 100 as gross_profit_percentage,
        gas_cost_usd as estimated_gas_cost_usd,
        (e2.bid - e1.ask) * LEAST(e1.bid_volume, e2.ask_volume, 1.0) - gas_cost_usd as net_profit_usd,
        ((e2.bid - e1.ask) / e1.ask * 100) - (gas_cost_usd / (e1.ask * LEAST(e1.bid_volume, e2.ask_volume, 1.0)) * 100) as net_profit_percentage,
        current_gas as current_gas_gwei,
        congestion_level as risk_level
    FROM 
        exchange_data e1
    JOIN 
        exchange_data e2 ON e1.symbol = e2.symbol AND e1.timestamp = e2.timestamp
    WHERE 
        e1.exchange != e2.exchange
        AND e2.bid > e1.ask
        AND (e2.bid - e1.ask) / e1.ask * 100 >= min_profit_percentage
        AND e1.timestamp >= DATE_SUB(NOW(), INTERVAL check_minutes MINUTE)
    GROUP BY 
        e1.symbol, e1.exchange, e2.exchange
    HAVING 
        MAX(e1.timestamp) >= DATE_SUB(NOW(), INTERVAL 1 MINUTE);
END//

-- Procedure to run all refresh operations
CREATE OR REPLACE PROCEDURE refresh_all_data()
BEGIN
    CALL refresh_network_status();
    CALL refresh_tx_stats_history();
    CALL refresh_price_comparison();
    CALL detect_arbitrage_opportunities(0.5, 5);
END//

DELIMITER ;

-- Create pipelines for Confluent Kafka
-- Note: Replace the connection details with your actual Confluent Cloud settings

-- Pipeline for exchange data
CREATE PIPELINE exchange_data_pipeline AS
LOAD DATA KAFKA 'your-confluent-bootstrap-server:9092/exchange-data'
CONFLUENT AUTHENTICATION WITH SECRET 'api_key' = 'YOUR_CONFLUENT_API_KEY' AND 'api_secret' = 'YOUR_CONFLUENT_API_SECRET'
INTO TABLE exchange_data
FORMAT JSON (
    timestamp <- timestamp,
    exchange <- exchange,
    symbol <- symbol,
    bid <- bid,
    ask <- ask,
    last <- last,
    bid_volume <- bid_volume,
    ask_volume <- ask_volume,
    spread <- spread,
    spread_percentage <- spread_percentage
);

-- Pipeline for blockchain data
CREATE PIPELINE blockchain_data_pipeline AS
LOAD DATA KAFKA 'your-confluent-bootstrap-server:9092/blockchain-data'
CONFLUENT AUTHENTICATION WITH SECRET 'api_key' = 'YOUR_CONFLUENT_API_KEY' AND 'api_secret' = 'YOUR_CONFLUENT_API_SECRET'
INTO TABLE blockchain_data
FORMAT JSON (
    timestamp <- timestamp,
    data_type <- data_type,
    
    -- Gas prices fields
    safe_gas_price <- safe_gas_price,
    propose_gas_price <- propose_gas_price,
    fast_gas_price <- fast_gas_price,
    last_block <- last_block,
    suggested_base_fee <- suggested_base_fee,
    
    -- Latest block fields
    block_number <- block_number,
    block_time <- block_time,
    gas_used <- gas_used,
    gas_limit <- gas_limit,
    transaction_count <- transaction_count,
    base_fee_per_gas <- base_fee_per_gas,
    
    -- Recent transaction fields
    tx_hash <- tx_hash,
    from_address <- from_address,
    to_address <- to_address,
    gas_price_gwei <- gas_price_gwei,
    value_eth <- value_eth,
    tx_timestamp <- tx_timestamp,
    tx_age_seconds <- tx_age_seconds,
    function_name <- function_name,
    
    -- Network metrics fields
    gas_usage_percentage <- gas_usage_percentage,
    block_fullness <- block_fullness,
    network_congestion <- network_congestion,
    
    -- Transaction statistics fields
    avg_gas_price <- avg_gas_price,
    tx_count <- tx_count,
    avg_tx_age_seconds <- avg_tx_age_seconds
);

-- Start the pipelines
START PIPELINE exchange_data_pipeline;
START PIPELINE blockchain_data_pipeline;
