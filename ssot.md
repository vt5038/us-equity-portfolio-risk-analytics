1. Project Name
US Equity Portfolio & Risk Analytics Platform
________________________________________
2. Purpose
To provide accurate, historical, and explainable insights into how a stock portfolio performs and behaves relative to the US market.
________________________________________
3. Target User
Portfolio analyst / investor / decision-maker
(analytics and evaluation, not trading or prediction)
________________________________________
4. Data Scope
•	Geography: United States
•	Frequency: Daily
•	Asset type: Equities
•	Benchmark: S&P 500 (via SPY ETF)
•	All dates are aligned to the US market trading calendar.
•	Timezone handling is not explicitly modeled in v1.

________________________________________
5. Data Sources
•	Market Data: Tiingo End-of-Day API
o	Stable daily data
o	Adjusted prices (splits/dividends handled)
o	Supports incremental ingestion
•	Portfolio Definition: External CSV input
________________________________________
6. Ingestion Pattern
•	Initial historical backfill
•	Daily incremental updates
•	Batch processing (not streaming)
Why batch:
Stock prices finalize daily; batch is correct, simpler, and cost-effective.
________________________________________
7. Storage & Architecture
•	Cloud: AWS
•	Object Storage: S3
•	Data Warehouse: Amazon Redshift
•	Orchestration: Apache Airflow
•	Transformations: AWS Glue (Spark)
________________________________________
8. Data Layers
•	Bronze: Raw API responses + raw portfolio CSV
•	Silver: Cleaned, validated, standardized datasets
•	Gold: Analytics-ready fact & dimension tables
________________________________________
9. Data Model (v1 — FROZEN)
Dimensions
•	dim_date — one row per calendar/trading date
•	dim_security — one row per equity (ticker), including SPY
•	dim_portfolio — one row per portfolio
Facts (with grain)
•	fact_market_price_daily
o	Grain: one row per (security, date)
•	fact_portfolio_position_daily
o	Grain: one row per (portfolio, security, date)
•	fact_portfolio_performance_daily
o	Grain: one row per (portfolio, date)
No other fact or dimension tables exist in v1.
________________________________________
10. KPI Definitions (business meaning)
Primary KPIs (computed daily, stored in Gold)
•	Portfolio Value
•	Daily Return
•	Cumulative Return
•	Volatility (rolling)
•	Maximum Drawdown
•	Beta vs Market (SPY)
Supporting KPIs (derived via queries/views)
•	Contribution to Return (by security)
•	Sector Exposure (%)
•	Sector-wise Performance
________________________________________
11. Portfolio Assumption (v1)
•	Portfolio is an external input, not system-generated
•	Provided as a static CSV file
•	Stored in Bronze, standardized in Silver
•	The system evaluates portfolios; it does not create or optimize them
•	Extensible to transaction-based portfolios in later versions
________________________________________
12. Benchmark Modeling Decision
•	SPY is treated as a regular security
•	Stored in dim_security
•	Daily prices stored in fact_market_price_daily
•	No separate benchmark tables in v1
________________________________________
13. Pipeline Characteristics
•	Deterministic, rule-based transformations
•	Date-partitioned processing
•	Idempotent reruns (safe to reprocess a given day)
Late-arriving data handling
•	The pipeline is date-partitioned and idempotent.
•	If market data arrives late or is corrected for a prior date, the affected date partitions are reprocessed and downstream tables are overwritten to maintain accuracy.
•	
________________________________________
14. Explicitly Out of Scope (v1)
•	Real-time or streaming pipelines
•	Trading or rebalancing logic
•	Machine learning or prediction
•	Alerts or notifications
•	CI/CD automation
________________________________________
15. Design Philosophy
•	Correctness over complexity
•	Explainability over buzzwords
•	Realistic over flashy
•	Extensible, not bloated
________________________________________
16. ETL Mapping (clarity)
Extract
•	Market prices from API
•	Portfolio CSV from S3
Transform
•	Clean and validate data
•	Align dates
•	Join portfolio holdings with prices
•	Compute values, returns, and risk metrics
Load
•	Persist analytics-ready tables in Redshift
This is a pure ETL pipeline.

17. Data Quality Assumptions (v1)
•	Basic validation checks are applied (nulls, data types, duplicates).
•	Invalid or incomplete records are logged and excluded from analytics.
•	Advanced data quality frameworks are out of scope for v1.

18. High-Level System Architecture
Purpose of the Architecture
To illustrate how data flows through the system from ingestion to analytics, and how each component fits into the overall ETL pipeline.
Architecture Overview (Narrative)
The system follows a batch-oriented, layered data architecture on AWS.
1.	Data Sources
o	Market data is pulled daily from the Tiingo End-of-Day API.
o	Portfolio definitions are provided as an external CSV file.
2.	Bronze Layer (Raw Ingestion)
o	Raw market API responses are stored in Amazon S3 without modification.
o	Raw portfolio CSV files are stored alongside market data as input artifacts.
3.	Silver Layer (Standardization & Cleaning)
o	Raw data is cleaned, validated, and standardized.
o	Schema consistency, data types, and basic quality checks are applied.
4.	Gold Layer (Analytics & KPIs)
o	Portfolio holdings are joined with daily market prices.
o	Portfolio-level and security-level analytics are computed.
o	Analytics-ready fact and dimension tables are stored in Amazon Redshift.
5.	Orchestration
o	Apache Airflow coordinates the end-to-end pipeline.
o	Supports historical backfill and daily incremental runs.
________________________________________
Architecture Diagram (for GitHub)
What the diagram should show visually:
•	Tiingo API → S3 Bronze
•	Portfolio CSV → S3 Bronze
•	S3 Bronze → Glue (Spark)
•	Glue → S3 Silver
•	Glue → Redshift Gold
•	Airflow orchestrating all steps
Suggested filename:
docs/architecture_diagram.png
Suggested caption (use this verbatim):
High-level architecture of the US Equity Portfolio & Risk Analytics Platform, illustrating batch ingestion, layered data processing, and analytics storage.
________________________________________
19. Data Modeling Approach
Modeling Philosophy
The project uses a star schema optimized for analytical queries, with a clear separation between:
•	descriptive attributes (dimensions)
•	measurable events (facts)
The model is designed to:
•	prevent double counting
•	support time-series analysis
•	enable flexible slicing by portfolio, security, and date
________________________________________
Dimension Tables (Conceptual Description)
dim_date
Represents the calendar and trading timeline.
Used for:
•	rolling metrics
•	drawdown calculations
•	time-based aggregations
dim_security
Represents equities traded in the US market.
Includes:
•	stock metadata (ticker, sector, industry)
•	benchmark representation (SPY)
dim_portfolio
Represents logical investment portfolios.
Allows:
•	multiple portfolios
•	scalable analytics without code changes
________________________________________
Fact Tables (Conceptual Description)
fact_market_price_daily
Captures daily market prices for each security.
Acts as the single source of market truth.
fact_portfolio_position_daily
Represents daily portfolio composition at the security level.
Enables:
•	exposure analysis
•	contribution analysis
•	sector aggregation
fact_portfolio_performance_daily
Represents daily portfolio-level performance.
Acts as the analytics scoreboard for KPIs.
________________________________________
Data Model Diagram (for GitHub)
What the diagram should show visually:
•	Central fact tables
•	Foreign-key relationships to dimensions
•	Grain annotations (important!)
Suggested filename:
docs/data_model_star_schema.png
Suggested caption:
Star schema data model used in the Gold layer, designed for portfolio performance and risk analytics.
________________________________________
20. Grain Definition (Explicit & Interview-Safe)
Table Name	Grain Description
dim_date	One row per calendar/trading date
dim_security	One row per equity (ticker)
dim_portfolio	One row per portfolio
fact_market_price_daily	One row per (security, date)
fact_portfolio_position_daily	One row per (portfolio, security, date)
fact_portfolio_performance_daily	One row per (portfolio, date)
________________________________________
21. Orchestration & Execution Flow
Pipeline Execution Modes
•	Historical backfill: loads past market data in batches
•	Incremental daily run: processes the most recent trading day
Execution Characteristics
•	Deterministic and repeatable
•	Date-partitioned processing
•	Safe to rerun for any given day
________________________________________
22. Error Handling & Observability (v1)
Error Handling
•	API failures are retried
•	Missing data is logged
•	Invalid records are excluded from Gold analytics
Observability
•	Airflow task-level logs
•	Row count checks between layers
•	Execution timestamps stored in metadata columns
________________________________________
23. Repository Structure (Recommended)
project-root/
├── dags/                  # Airflow DAGs
├── glue_jobs/             # Spark transformation scripts
├── sql/                   # Redshift DDL & analytics queries
├── data_samples/          # Example portfolio CSV
├── docs/
│   ├── architecture_diagram.png
│   ├── data_model_star_schema.png
│   └── design_notes.md
├── README.md
└── ssot.md
________________________________________
24. README Structure (Suggested)
Your GitHub README should roughly follow:
1.	Problem Statement (non-technical)
2.	System Architecture (with diagram)
3.	Data Model (with diagram)
4.	ETL Pipeline Flow
5.	KPIs & Business Insights
6.	Design Decisions & Trade-offs
7.	Limitations & Future Enhancements
