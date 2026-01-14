SELECT TOP 10 *
FROM OPENROWSET(
    BULK 'Files/silver/retail_transactions.parquet',
    FORMAT = 'PARQUET'
) AS t;


-- ========================
-- Base retail data (Silver)
-- ========================

CREATE OR ALTER VIEW vw_retail_base AS
SELECT
    invoice,
    stockcode,
    description,
    quantity,
    price,
    revenue,
    invoicedate,
    customer_id,
    country,
    is_cancelled,
    is_return,
    is_zero_price,
    is_non_product,
    is_missing_date
FROM OPENROWSET(
    BULK 'Files/silver/retail_transactions.parquet',
    FORMAT = 'PARQUET'
) AS t;

CREATE OR ALTER VIEW vw_retail_net_sales AS
SELECT
    *,
    CASE
        WHEN is_cancelled = 0
         AND is_return = 0
         AND is_zero_price = 0
         AND is_non_product = 0
        THEN revenue
    END AS net_revenue
FROM vw_retail_base;

CREATE OR ALTER VIEW vw_retail_time_sales AS
SELECT *
FROM vw_retail_net_sales;



-- ========================
-- Monthly KPIs
-- ========================


CREATE OR ALTER VIEW vw_monthly_revenue AS
WITH ParsedDates AS (
    SELECT 
        net_revenue,
        -- Divide by 1,000,000,000 to get seconds, then convert to Date
        CASE 
            WHEN invoicedate IS NOT NULL AND invoicedate > 0 
            THEN DATEADD(SECOND, CAST(invoicedate / 1000000000 AS BIGINT), '1970-01-01')
            ELSE NULL 
        END AS confirmed_date
    FROM vw_retail_net_sales
)
SELECT
    CAST(COALESCE(DATEFROMPARTS(YEAR(confirmed_date), MONTH(confirmed_date), 1), '1900-01-01') AS DATE) AS year_month_date,
    COALESCE(FORMAT(confirmed_date, 'yyyy-MM'), 'Unknown') AS year_month,
    SUM(ISNULL(net_revenue, 0)) AS monthly_revenue
FROM ParsedDates
GROUP BY 
    COALESCE(DATEFROMPARTS(YEAR(confirmed_date), MONTH(confirmed_date), 1), '1900-01-01'),
    COALESCE(FORMAT(confirmed_date, 'yyyy-MM'), 'Unknown');


CREATE OR ALTER VIEW vw_monthly_revenue_growth AS
SELECT
    year_month_date,
    year_month,
    monthly_revenue,
    -- Handle the first month change as 0 instead of NULL
    COALESCE(monthly_revenue - LAG(monthly_revenue) OVER (ORDER BY year_month_date), 0) AS mom_change,
    -- Safe division to prevent overflow/divide-by-zero
    CAST(
        COALESCE(
            (monthly_revenue - LAG(monthly_revenue) OVER (ORDER BY year_month_date)) 
            / NULLIF(LAG(monthly_revenue) OVER (ORDER BY year_month_date), 0), 
        0) 
    AS DECIMAL(18,4)) AS mom_growth_pct
FROM vw_monthly_revenue;
