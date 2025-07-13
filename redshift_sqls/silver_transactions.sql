-- CREATE TABLE "dev"."pagefly"."silver_transactions" AS
WITH transactions AS (
    SELECT
        createdat,
        id,
        CAST(REGEXP_SUBSTR(id, '[0-9]+$') AS INT) AS _id,
        chargeid,
        netamount,
        shopid,
        LAG(netamount) OVER (PARTITION BY shopid ORDER BY _id) AS prev_netamount
    FROM "glue_catalog_schema_partnerapi"."pagefly_transactions_main_curated"
    WHERE shopid = 'gid://partners/Shop/54926180530'
    ORDER BY createdat, id
)
, base_events AS (
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
        AND shopid = 'gid://partners/Shop/54926180530'
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
, joined2 AS (
    SELECT
        f.createdat,
        f.id,
        f._id,
        f.netamount,
        f.prev_netamount,
        f.shopid,
        s.pricing_plan,
        s.chargeamount,
        s.activated_date,
        s.actual_end_date,
        s.prev_pricing_plan,
        s.prev_chargeamount,
        s.prev_activated_date,
        s.prev_actual_end_date,
        ROW_NUMBER() OVER (
            PARTITION BY f.id 
            ORDER BY 
            CASE
                WHEN f.createdat
                BETWEEN s.activated_date AND s.actual_end_date THEN 0
                WHEN f.createdat > s.actual_end_date
            THEN DATEDIFF(day, s.actual_end_date, f.createdat)
            ELSE 999999
        END,
        s.activated_date,
        s.actual_end_date
        ) AS rn
    FROM transactions AS f
    LEFT JOIN (
        SELECT DISTINCT
            *,
            LAG(pricing_plan) OVER (PARTITION BY shopid ORDER BY activated_date) AS prev_pricing_plan,
            LAG(chargeamount) OVER (PARTITION BY shopid ORDER BY activated_date) AS prev_chargeamount,
            LAG(activated_date) OVER (PARTITION BY shopid ORDER BY activated_date) AS prev_activated_date,
            LAG(actual_end_date) OVER (PARTITION BY shopid ORDER BY activated_date) AS prev_actual_end_date
        FROM subscriptions
    ) AS s
        ON f.shopid = s.shopid
)
,matching AS (
SELECT 
    createdat, id, _id,netamount, prev_netamount, shopid,
    SPLIT_TO_ARRAY(
        LISTAGG(pricing_plan, ',')
        WITHIN GROUP (ORDER BY rn)
        ) AS matching_plan,
    SPLIT_TO_ARRAY(
        LISTAGG(prev_pricing_plan, ',')
        WITHIN GROUP (ORDER BY rn)
    ) AS matching_prev_pricing_plan,
    SPLIT_TO_ARRAY(
        LISTAGG(chargeamount, ',')
        WITHIN GROUP (ORDER BY rn)
    ) AS matching_chargeamount,
    SPLIT_TO_ARRAY(
        LISTAGG(prev_chargeamount, ',')
        WITHIN GROUP (ORDER BY rn)
    ) AS matching_prev_chargeamount,
    SPLIT_TO_ARRAY(
        LISTAGG(activated_date, ',')
        WITHIN GROUP (ORDER BY rn)
    ) AS matching_activated_date,
    SPLIT_TO_ARRAY(
        LISTAGG(actual_end_date, ',')
        WITHIN GROUP (ORDER BY rn)
    ) AS matching_actual_end_date,
    SPLIT_TO_ARRAY(
        LISTAGG(prev_activated_date, ',')
        WITHIN GROUP (ORDER BY rn)
    ) AS matching_prev_activated_date,
    SPLIT_TO_ARRAY(
        LISTAGG(prev_actual_end_date, ',')
        WITHIN GROUP (ORDER BY rn)
    ) AS matching_prev_actual_end_date,
    LAG(netamount) OVER (PARTITION BY shopid ORDER BY createdat, id) AS prev_day_amount,
    COALESCE(
            FIRST_VALUE(netamount) OVER (
                PARTITION BY shopid 
                ORDER BY EXTRACT(epoch FROM createdat), id
                ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING)
            ,
            LEAD(netamount) OVER (PARTITION BY shopid ORDER BY EXTRACT(epoch FROM createdat), id)
        ) AS next_day_amount,
        COUNT(id) OVER (PARTITION BY shopid, createdat::DATE) AS transactions_per_day
FROM joined2
GROUP BY createdat, id, _id,netamount, prev_netamount, shopid
ORDER BY createdat
)
,extracting AS (
    SELECT 
        createdat, id, _id,netamount, prev_netamount, next_day_amount,shopid,transactions_per_day,
        CASE
            WHEN
                t.matching_prev_pricing_plan[0] IS NOT NULL
                AND t.transactions_per_day > 1
                AND EXISTS (
                  SELECT 1
                  FROM t.matching_prev_actual_end_date AS mp_end_date
                  WHERE date_trunc('month', mp_end_date::timestamp)
                  = date_trunc('month', t.createdat)
                )
                AND t.netamount = t.prev_day_amount
              THEN t.matching_prev_pricing_plan[0]
            WHEN 
              t.matching_prev_pricing_plan[0] IS NOT NULL 
              AND t.transactions_per_day > 1
              AND EXISTS (
                SELECT 1
                FROM t.matching_prev_actual_end_date AS mp_end_date
                WHERE datediff(
                  'month',
                  date_trunc('month', mp_end_date::timestamp),
                  date_trunc('month', t.createdat)
                ) = 1
              )
              AND t.netamount <> t.next_day_amount
            THEN t.matching_prev_pricing_plan[0]
            ELSE t.matching_plan [0]
            END AS pricing_plan,

        CASE
            WHEN
                t.matching_prev_chargeamount[0] IS NOT NULL
                AND t.transactions_per_day > 1
                AND EXISTS (
                  SELECT 1
                  FROM t.matching_prev_actual_end_date AS mp_end_date
                  WHERE date_trunc('month', mp_end_date::timestamp)
                  = date_trunc('month', t.createdat)
                )
                AND t.netamount = t.prev_day_amount
              THEN t.matching_prev_chargeamount[0]
            WHEN 
              t.matching_prev_chargeamount[0] IS NOT NULL 
              AND t.transactions_per_day > 1
              AND EXISTS (
                SELECT 1
                FROM t.matching_prev_actual_end_date AS mp_end_date
                WHERE datediff(
                  'month',
                  date_trunc('month', mp_end_date::timestamp),
                  date_trunc('month', t.createdat)
                ) = 1
              )
              AND t.netamount <> t.next_day_amount
            THEN t.matching_prev_chargeamount[0]
            ELSE t.matching_chargeamount[0]
            END AS chargeamount,
        CASE
            WHEN
                t.matching_prev_activated_date[0] IS NOT NULL
                AND t.transactions_per_day > 1
                AND EXISTS (
                  SELECT 1
                  FROM t.matching_prev_actual_end_date AS mp_end_date
                  WHERE date_trunc('month', mp_end_date::timestamp)
                  = date_trunc('month', t.createdat)
                )
                AND t.netamount = t.prev_day_amount
              THEN t.matching_prev_activated_date[0]
            WHEN 
              t.matching_prev_activated_date[0] IS NOT NULL 
              AND t.transactions_per_day > 1
              AND EXISTS (
                SELECT 1
                FROM t.matching_prev_actual_end_date AS mp_end_date
                WHERE datediff(
                  'month',
                  date_trunc('month', mp_end_date::timestamp),
                  date_trunc('month', t.createdat)
                ) = 1
              )
              AND t.netamount <> t.next_day_amount
            THEN t.matching_prev_activated_date[0]
            ELSE t.matching_activated_date[0]
            END AS activated_date,
         CASE
            WHEN
                t.matching_prev_actual_end_date[0] IS NOT NULL
                AND t.transactions_per_day > 1
                AND EXISTS (
                  SELECT 1
                  FROM t.matching_prev_actual_end_date AS mp_end_date
                  WHERE date_trunc('month', mp_end_date::timestamp)
                  = date_trunc('month', t.createdat)
                )
                AND t.netamount = t.prev_day_amount
              THEN t.matching_prev_actual_end_date[0]
            WHEN 
              t.matching_prev_actual_end_date[0] IS NOT NULL 
              AND t.transactions_per_day > 1
              AND EXISTS (
                SELECT 1
                FROM t.matching_prev_actual_end_date AS mp_end_date
                WHERE datediff(
                  'month',
                  date_trunc('month', mp_end_date::timestamp),
                  date_trunc('month', t.createdat)
                ) = 1
              )
              AND t.netamount <> t.next_day_amount
            THEN t.matching_prev_actual_end_date[0]
            ELSE t.matching_actual_end_date[0]
            END AS actual_end_date           
    FROM matching t
)
,type_pricing AS (
    SELECT 
        createdat,
        DATE_TRUNC('month', createdat) AS month,
        id,_id,netamount, prev_netamount,next_day_amount,shopid,transactions_per_day,
        CAST(
                (CASE 
                WHEN id LIKE '%AppOneTimeSale%' THEN 'App One Time Sale'
                WHEN id LIKE '%Adjustment%' AND pricing_plan IS NULL THEN 'Undefined refund'
                WHEN id LIKE '%AppSubscriptionSale%' AND pricing_plan IS NULL THEN 'Undefined transactions'
                WHEN id LIKE '%AppSaleCredit%' THEN 'App Sale Credit'
                ELSE pricing_plan
                END)
            AS VARCHAR
            ) AS pricing_plan,
        CAST(chargeamount AS DOUBLE PRECISION) AS chargeamount,
        CAST(activated_date AS DATE) AS activated_date,
        CAST(actual_end_date AS DATE) AS actual_end_date
FROM extracting
)
,modified AS (
SELECT 
    createdat, 
    DATE_TRUNC('month', createdat) AS month, 
    id,_id,netamount, prev_netamount,next_day_amount,shopid,transactions_per_day,
    CASE 
        WHEN netamount = prev_netamount 
        THEN LAG(pricing_plan) OVER (PARTITION BY shopid ORDER BY _id,createdat)
        ELSE pricing_plan
    END AS pricing_plan,
    CASE 
        WHEN netamount = prev_netamount 
        THEN LAG(chargeamount) OVER (PARTITION BY shopid ORDER BY _id,createdat)
        ELSE chargeamount
    END AS chargeamount,
    CASE 
        WHEN netamount = prev_netamount 
        THEN LAG(activated_date) OVER (PARTITION BY shopid ORDER BY _id,createdat)
        ELSE activated_date
    END AS activated_date,
    CASE 
        WHEN netamount = prev_netamount 
        THEN LAG(actual_end_date) OVER (PARTITION BY shopid ORDER BY _id,createdat)
        ELSE actual_end_date
    END AS actual_end_date
FROM type_pricing
-- WHERE id LIKE '%222828418%'
ORDER BY _id
)
,lagging AS (
SELECT 
    *,
    LAG(pricing_plan) OVER (PARTITION BY shopid ORDER BY _id) AS prev_pricing_plan,
    LAG(chargeamount) OVER (PARTITION BY shopid ORDER BY _id) AS prev_chargeamount
FROM modified
ORDER BY _id
)
SELECT 
    *
FROM lagging