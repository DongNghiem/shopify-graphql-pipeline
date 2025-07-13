WITH transactions AS (
    SELECT
        createdat,
        id,
        CAST(REGEXP_SUBSTR(id, '[0-9]+$') AS INT) AS _id,
        chargeid,
        netamount,
        shopid
    FROM "glue_catalog_schema_partnerapi"."pagefly_transactions_main_curated"
    -- WHERE shopid = 'gid://partners/Shop/54926180530'
    ORDER BY createdat, id
)
,additional AS (
    SELECT 
        DATE_TRUNC('month', createdat) AS month,
        shopid,
        SUM(netamount) AS revenue
    FROM transactions
    GROUP BY month, shopid
    ORDER BY month,shopid
)
,rev_lag AS (
SELECT 
    *,
    LAG(revenue) OVER (PARTITION BY shopid ORDER BY month) AS prev_month_revenue
FROM additional
)
,shop_rev AS (
SELECT 
    *,
    CASE 
        WHEN prev_month_revenue IS NULL THEN revenue 
        ELSE 0.0
    END AS new_mrr,
    CASE 
        WHEN revenue = prev_month_revenue THEN revenue 
        WHEN revenue > prev_month_revenue THEN prev_month_revenue
        WHEN revenue < prev_month_revenue THEN revenue
        ELSE 0.0
    END AS mrr,
    CASE 
        WHEN revenue > prev_month_revenue THEN revenue - prev_month_revenue
        ELSE 0.0
    END AS expansion_mrr,
    CASE 
        WHEN revenue < prev_month_revenue THEN revenue - prev_month_revenue
        ELSE 0.0
    END AS contraction_mrr,
    0.0 AS churn_mrr
    -- SUM(revenue) OVER (PARTITION BY shopid ORDER BY month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS total_revenue_overtime
FROM rev_lag
ORDER BY shopid, month
)
,last_month AS (
    SELECT 
    DISTINCT
        shopid,
        FIRST_VALUE(month) OVER (PARTITION BY shopid ORDER BY month DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS final_month,
        FIRST_VALUE(mrr) OVER (PARTITION BY shopid ORDER BY month DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS final_mrr
    FROM shop_rev
)
,onion AS (
SELECT 
    month,
    shopid,
    revenue,
    prev_month_revenue,
    new_mrr,
    mrr,
    expansion_mrr,
    contraction_mrr,
    churn_mrr
FROM shop_rev
UNION ALL
SELECT 
    DATEADD(month, 1, final_month) AS month,
    shopid,
    0.0 AS revenue,
    NULL AS prev_month_revenue,
    0.0 AS new_mrr,
    0.0 AS mrr,
    0.0 AS expansion_mrr,
    0.0 AS contraction_mrr,
    CASE
        WHEN DATEADD(month, 1, final_month) = DATE_TRUNC('month', CURRENT_DATE)
        OR DATEADD(month, 1, final_month) = DATEADD(month, 1, DATE_TRUNC('month', CURRENT_DATE))
        THEN 0.0
        ELSE -(final_mrr) 
    END AS churn_mrr
FROM last_month
)
SELECT 
    month,
    SUM(mrr) AS mrr,
    SUM(new_mrr) AS new_mrr,
    SUM(expansion_mrr) AS expansion_mrr,
    SUM(contraction_mrr) AS contraction_mrr,
    CASE 
        WHEN month = DATE_TRUNC('month', CURRENT_DATE) OR 
        month = DATEADD(month, 1, CURRENT_DATE) THEN 0.0 
        ELSE SUM(churn_mrr) 
    END AS churn_mrr
FROM onion
GROUP BY month 
ORDER BY month