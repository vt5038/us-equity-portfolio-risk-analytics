ALTER TABLE fact_portfolio_performance_daily ADD COLUMN cumulative_return DOUBLE PRECISION;
ALTER TABLE fact_portfolio_performance_daily ADD COLUMN vol_21d_ann DOUBLE PRECISION;
ALTER TABLE fact_portfolio_performance_daily ADD COLUMN vol_63d_ann DOUBLE PRECISION;
ALTER TABLE fact_portfolio_performance_daily ADD COLUMN drawdown DOUBLE PRECISION;
ALTER TABLE fact_portfolio_performance_daily ADD COLUMN max_drawdown_to_date DOUBLE PRECISION;
ALTER TABLE fact_portfolio_performance_daily ADD COLUMN beta_63d DOUBLE PRECISION;
