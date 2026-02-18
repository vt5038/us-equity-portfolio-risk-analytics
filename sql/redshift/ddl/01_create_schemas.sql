-- External schema for Silver market prices
CREATE EXTERNAL SCHEMA spectrum_silver
FROM DATA CATALOG
DATABASE 'us_equity_silver'
IAM_ROLE 'arn:aws:iam::654654501953:role/redshift_copy_s3_role'
REGION 'us-east-1';

-- External schema for Silver portfolio holdings
CREATE EXTERNAL SCHEMA spectrum_porta_risk
FROM DATA CATALOG
DATABASE 'porta_risk_silver'
IAM_ROLE 'arn:aws:iam::654654501953:role/redshift_copy_s3_role'
REGION 'us-east-1';