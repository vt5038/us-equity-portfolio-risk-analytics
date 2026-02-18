-- Market prices Silver validation
SELECT MIN(trade_date) AS min_dt, MAX(trade_date) AS max_dt, COUNT(*) AS total_rows
FROM silver_market_price_daily;

-- Example spot checks
SELECT ticker, adj_close
FROM silver_market_price_daily
WHERE trade_date = DATE '2024-01-10'
ORDER BY ticker;

SELECT trade_date, adj_close
FROM silver_market_price_daily
WHERE ticker = 'SPY'
ORDER BY trade_date DESC
LIMIT 5;