WITH base_events AS (
    SELECT
        eventtype,
        occurredat,
        shopdomain,
        shopid,
        chargeamount,
        chargeid,
        chargename
    FROM "glue_catalog_schema_partnerapi"."pagefly_events_main_curated"
    WHERE 1 = 1
        AND eventtype LIKE '%SUBSCRIPTION_CHARGE%'
        -- AND shopid = 'gid://partners/Shop/54926180530'
    ORDER BY occurredat
)
, event_ranks AS (
    SELECT
        eventtype,
        occurredat,
        shopid,
        shopdomain,
        chargename,
        chargeid,
        chargeamount,
        FIRST_VALUE(occurredat) OVER (
            PARTITION BY shopid, chargeid
            ORDER BY occurredat ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS activated_date,
        LEAD(occurredat) OVER (PARTITION BY shopid, chargeid ORDER BY occurredat) AS deactivated_date
    FROM base_events
    WHERE 1 = 1 
    AND shopid IS NOT NULL
)
, filtered AS (
    SELECT
        *,
        LEAD(activated_date) OVER (PARTITION BY shopid ORDER BY activated_date) AS next_activation
    FROM event_ranks
    WHERE eventtype IN ('SUBSCRIPTION_CHARGE_ACTIVATED', 'SUBSCRIPTION_CHARGE_UNFROZEN')
)
, subscriptions AS (
    SELECT
        shopid,
        shopdomain,
        chargeid,
        activated_date,
        chargeamount,
        CASE
            WHEN chargename LIKE '%Free%'
                THEN 'Unlimited Blog posts'
            ELSE chargename
        END AS pricing_plan,
        CASE
            WHEN next_activation < deactivated_date THEN next_activation
            WHEN deactivated_date IS NULL AND next_activation IS NOT NULL THEN next_activation
            WHEN deactivated_date IS NULL AND next_activation IS NULL THEN DATEADD(day, 1, CURRENT_DATE)
            ELSE deactivated_date
        END AS actual_end_date
    FROM filtered
)
,numbers AS (
SELECT ones.n + tens.n * 10 + hundreds.n * 100 + thousands.n * 1000 AS n
FROM 
  (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
   UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) AS ones,
  (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
   UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) AS tens,
  (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
   UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) AS hundreds,
  (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
   UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) AS thousands
)
,expanded_date AS (
    SELECT 
        s.shopid,
        s.shopdomain,
        s.pricing_plan,
        DATE_TRUNC('day',s.activated_date) AS activated_date,
        DATE_TRUNC('day',s.actual_end_date) AS actual_end_date,
        DATEADD(day, n.n, DATE_TRUNC('day',s.activated_date)) AS active_date
    FROM subscriptions s
    JOIN numbers n 
    ON DATEADD(day, n.n, DATE_TRUNC('day',s.activated_date)) <= 
        (
        CASE 
            WHEN s.actual_end_date IS NULL THEN DATEADD(day, 1, CURRENT_DATE)
            ELSE DATE_TRUNC('day', actual_end_date) - 1
            END
        )
)
,counting AS (
SELECT 
    active_date,
    COUNT(DISTINCT shopid) AS merchants
FROM expanded_date
GROUP BY active_date
ORDER BY active_date DESC
)
,extracting AS (
SELECT 
  *,
  LAG(merchants) OVER (ORDER BY active_date) AS prev_day_merchants,
  DATE_TRUNC('month', active_date) AS month,
  LAST_VALUE(merchants) OVER (PARTITION BY DATE_TRUNC('month', active_date) ORDER BY active_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS end_of_month_merchants
FROM counting
WHERE active_date <= CURRENT_DATE
ORDER BY active_date
)
,dis AS (
  SELECT
    DISTINCT 
    month,
    end_of_month_merchants
  FROM extracting
)
,lagging AS (
  SELECT 
    *,
    LAG(end_of_month_merchants) OVER (ORDER BY month) AS last_month_merchants
  FROM dis
)
,final AS (
SELECT 
  e.*,
  l.last_month_merchants
FROM extracting e
LEFT JOIN lagging l ON e.month = l.month
ORDER BY e.month DESC
)
SELECT 
  *
FROM final