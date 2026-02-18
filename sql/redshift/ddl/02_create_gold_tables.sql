-- Gold: market prices fact
CREATE TABLE IF NOT EXISTS fact_market_price_daily (
  trade_date      DATE         NOT NULL,
  ticker          VARCHAR(20)  NOT NULL,
  "open"          DOUBLE PRECISION,
  "high"          DOUBLE PRECISION,
  "low"           DOUBLE PRECISION,
  "close"         DOUBLE PRECISION,
  adj_close       DOUBLE PRECISION NOT NULL,
  volume          BIGINT,
  adj_volume      BIGINT,
  ingested_at_utc TIMESTAMP,
  source_system   VARCHAR(50),
  PRIMARY KEY (trade_date, ticker)
);

-- Gold dimensions
CREATE TABLE IF NOT EXISTS dim_security (
  security_id INTEGER IDENTITY(1,1),
  ticker      VARCHAR(20) NOT NULL,
  PRIMARY KEY (security_id),
  UNIQUE (ticker)
);

CREATE TABLE IF NOT EXISTS dim_date (
  date_id    INTEGER IDENTITY(1,1),
  trade_date DATE NOT NULL,
  PRIMARY KEY (date_id),
  UNIQUE (trade_date)
);

CREATE TABLE IF NOT EXISTS dim_portfolio (
  portfolio_id   INTEGER IDENTITY(1,1),
  portfolio_name VARCHAR(100) NOT NULL,
  as_of_date     DATE NOT NULL,
  PRIMARY KEY (portfolio_id),
  UNIQUE (portfolio_name, as_of_date)
);

CREATE TABLE IF NOT EXISTS fact_portfolio_position_daily (
  portfolio_id     INTEGER NOT NULL,
  security_id      INTEGER NOT NULL,
  date_id          INTEGER NOT NULL,
  as_of_date       DATE    NOT NULL,
  quantity         DOUBLE PRECISION NOT NULL,
  adj_close        DOUBLE PRECISION NOT NULL,
  position_value   DOUBLE PRECISION NOT NULL,
  PRIMARY KEY (portfolio_id, security_id, date_id)
);


CREATE TABLE IF NOT EXISTS fact_portfolio_performance_daily (
  portfolio_id     INTEGER NOT NULL,
  date_id          INTEGER NOT NULL,
  trade_date       DATE    NOT NULL,
  portfolio_value  DOUBLE PRECISION NOT NULL,
  daily_return     DOUBLE PRECISION,
  PRIMARY KEY (portfolio_id, date_id)
);