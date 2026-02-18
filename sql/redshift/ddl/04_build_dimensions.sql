-- Rebuild dim_security
TRUNCATE dim_security;
INSERT INTO dim_security (ticker)
SELECT DISTINCT ticker
FROM fact_market_price_daily
ORDER BY ticker;

-- Rebuild dim_date
TRUNCATE dim_date;
INSERT INTO dim_date (trade_date)
SELECT DISTINCT trade_date
FROM fact_market_price_daily
ORDER BY trade_date;

-- Rebuild dim_portfolio from Silver portfolio holdings (Glue porta_risk)
DROP TABLE IF EXISTS dim_portfolio;
CREATE TABLE dim_portfolio (
  portfolio_id   INTEGER IDENTITY(1,1),
  portfolio_name VARCHAR(100) NOT NULL,
  as_of_date     DATE NOT NULL,
  PRIMARY KEY (portfolio_id),
  UNIQUE (portfolio_name, as_of_date)
);

INSERT INTO dim_portfolio (portfolio_name, as_of_date)
SELECT DISTINCT portfolio_name, as_of_date
FROM spectrum_porta_risk.portfolio_holdings
ORDER BY portfolio_name;

SELECT COUNT(*) FROM dim_security;
SELECT COUNT(*) FROM dim_date;
SELECT COUNT(*) FROM dim_portfolio;