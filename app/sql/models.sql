CREATE TABLE exchange (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    base_currency TEXT NOT NULL,  -- Example: USD, USDT
    fee_structure JSONB NOT NULL  -- Stores taker/maker fees
);

CREATE TABLE price_snapshot (
    id SERIAL PRIMARY KEY,
    exchange_id INT REFERENCES exchange(id),
    asset TEXT NOT NULL,  -- Example: BTC/USDT
    bid_price DECIMAL NOT NULL,
    ask_price DECIMAL NOT NULL,
    spread DECIMAL GENERATED ALWAYS AS (ask_price - bid_price) STORED, 
    timestamp TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE trade (
    id SERIAL PRIMARY KEY,
    exchange_id INT REFERENCES exchange(id),
    asset TEXT NOT NULL,
    trade_type TEXT CHECK (trade_type IN ('buy', 'sell')),
    price DECIMAL NOT NULL,
    volume DECIMAL NOT NULL,
    fee DECIMAL NOT NULL,
    timestamp TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE order_book (
    id SERIAL PRIMARY KEY,
    exchange_id INT REFERENCES exchange(id),
    asset TEXT NOT NULL,
    bid_price DECIMAL NOT NULL,
    ask_price DECIMAL NOT NULL,
    bid_volume DECIMAL NOT NULL,
    ask_volume DECIMAL NOT NULL,
    timestamp TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE liquidity (
    id SERIAL PRIMARY KEY,
    exchange_id INT REFERENCES exchange(id),
    asset TEXT NOT NULL,
    order_book_id INT REFERENCES order_book(id),
    depth DECIMAL NOT NULL,  -- Total volume available at best price
    market_impact DECIMAL NOT NULL, -- Slippage if X volume is traded
    timestamp TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE transfer (
    id SERIAL PRIMARY KEY,
    from_exchange INT REFERENCES exchange(id),
    to_exchange INT REFERENCES exchange(id),
    asset TEXT NOT NULL,
    amount DECIMAL NOT NULL,
    fee DECIMAL NOT NULL,
    duration INTERVAL NOT NULL,  -- Withdrawal delay
    status TEXT CHECK (status IN ('pending', 'completed', 'failed')),
    timestamp TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE arbitrage_ticket (
    id SERIAL PRIMARY KEY,
    asset TEXT NOT NULL,
    buy_exchange INT REFERENCES exchange(id),  -- Cheapest exchange
    sell_exchange INT REFERENCES exchange(id), -- Most expensive exchange
    buy_price DECIMAL NOT NULL,
    sell_price DECIMAL NOT NULL,
    profit DECIMAL GENERATED ALWAYS AS (sell_price - buy_price) STORED, 
    spread_percentage DECIMAL GENERATED ALWAYS AS ((sell_price - buy_price) / buy_price * 100) STORED,
    status TEXT CHECK (status IN ('open', 'executed', 'expired')),
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);                                                                                                                                                i   16  Bot  16:2    12:42
