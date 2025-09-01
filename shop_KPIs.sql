# ##############################################################################
# Author: Merlin Mary John
# Query: Tables `transaction.pending_bolts` and `transaction.ledger` in BigQuery
#        contains time series data. `transaction.pending_bolts` contains 
#        shopping details of users and `transaction.ledger` contains associated 
#        ledger for the purchases.  `transaction.pending_bolts` has a metadata 
#        field which includes metadata json received from affiliate end and has 
#        to be processed.
# 
# This is an advanced query example that includes GROUP BY, ORDER BY,
# json parsing, AGGREGATIONS (SUM, COUNT) , Conditional queries (CASE-WHEN),
# DATE Functions (DATE_TRUNC, ), REGEX parsing, INNER joins,  
# Subqueries, CTEs(Common Table Expression), and window_functions (ROW_NUMBER)
# 
# ##############################################################################

with 
  pendingBolts as (
    SELECT 
      * EXCEPT (metadata, metadata1),
      CASE
        WHEN shoppingCartAmountUsd IS NULL THEN shoppingCartAmount
        ELSE shoppingCartAmountUsd
      END sales,
      SAFE_CAST(
        CASE 
          #casting to float type and extracting cart amount
          WHEN shoppingCartAmountUsd IS NULL 
            THEN JSON_EXTRACT(metadata1, '$.shopInfo.shoppingCartAmount') 
          ELSE JSON_VALUE(metadata1, '$.shopInfo.shoppingCartAmountUsd') 
        END AS FLOAT64
      ) AS cart_amount,
      DATE_TRUNC(purchaseDate, WEEK) as startDateWeek,
      DATE_TRUNC(purchaseDate, MONTH) as startDateMonth    
    FROM (
      SELECT 
        *,
        JSON_VALUE(metadata1, '$.shopInfo.currency') cartCurrency, 
        CAST(
          JSON_VALUE(metadata1, '$.shopInfo.shoppingCartAmount') AS float64
        ) shoppingCartAmount,
        CAST(
          JSON_VALUE(metadata1, '$.shopInfo.shoppingCartAmountUsd') AS float64
        ) shoppingCartAmountUsd,
        CASE 
          WHEN 
            TRIM(
              JSON_VALUE(metadata1, '$.shopInfo.purchaseDate')
            ) = 'InvaliddateZ' 
            THEN NULL
          WHEN 
            JSON_VALUE(metadata1, '$.shopInfo.purchaseDate') LIKE '%T%'
            THEN 
              DATE(CAST(REPLACE(LEFT(
                JSON_VALUE(metadata1, '$.shopInfo.purchaseDate'), 19
              ), 'T', ' ') AS timestamp)) 
          ELSE 
            DATE(CAST(
              JSON_VALUE(metadata, '$.shopInfo.purchaseDate') AS timestamp)) 
        END purchaseDate      
      FROM (        
        SELECT 
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                REGEXP_REPLACE(
                  REPLACE(
                    REPLACE(
                      REPLACE(
                        `metadata`,
                        'True',
                        'true'
                      ), 'False',
                      'false'
                    ), 'None',
                    'null'
                  ), r'([^\p{ASCII}]+)',
                  ''
                ), r'\s',
                ''
              ), r'\*[\"]\w*[\"]', 
              ''
            ), r'\w+[\"]\w+',
            ''
          ) AS metadata1,
          uuid as uuid,
          account_id AS user_id,
          metadata,
          CASE status_id #naming transaction status
            WHEN 0 THEN 'Started'
            WHEN 1 THEN 'Pending'
            WHEN 4 THEN 'Paid'                   
            WHEN 5 THEN 'Failed'
            WHEN 6 THEN 'Rejected'
            WHEN 9 THEN 'Cancelled'
            WHEN 10 THEN 'Expired'
            ELSE NULL
          END AS Shopping_Status,
          status_id,
          INITCAP(
            JSON_EXTRACT_SCALAR(metadata, '$.shopInfo.affiliate')
          ) AS Affiliate,
          brand_name AS shop_name,
          DATE(updated_ts) AS updated_date,
          amount AS reward_amount,
          DATE(created_ts) AS transaction_date,
          source_id as pendingsourceid,
          source_sub_id as pendingsourcesubid
        FROM (
          SELECT
            *
          FROM (
            SELECT 
              uuid,
              account_id,
              status_id,
              source_id,
              created_ts,
              brand_name,
              amount,
              updated_ts,
              source_sub_id,
              operationType,
              metadata
            FROM `transaction.pending_bolts`  #path to BigQuery table
            QUALIFY ROW_NUMBER() OVER(PARTITION BY id ORDER BY loadTime DESC) = 1
          )
          WHERE 
            operationType != 'delete' 
            and status_id in (1,4,5,6)
        )
      )
    ) 
  ),
  pendingBolts2 as (
    SELECT distinct * FROM pendingBolts
  ),
  ledgerExtract as (
    SELECT
      *
    FROM (
      SELECT
        country,
        currency_id,
        exchange_rate,
        type_id,
        source_type_id AS ledger_type_id,
        amount AS ledger_amount,
        device_name AS device,
        platform_id AS platform,
        uuid as ledger_uuid,
        source_sub_id as ledger_source_sub_id,
        source_id as ledger_source_id,
        operationType,
        created_ts as ledger_created_ts,
        loadTime as ledger_load_time
      FROM
        `transaction.ledger` #path to BigQuery table
      WHERE
        type_id= 1
        and source_type_id=16
      QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY loadTime DESC) = 1
    )
    WHERE
      operationType!='delete'
      and ledger_created_ts > "2021-06-01"
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY ledger_source_id, ledger_source_sub_id
      ORDER BY ledger_load_time DESC
    ) = 1
  ),
  join1 as (
    SELECT 
      * 
    FROM (
      (SELECT * FROM pendingBolts2) pb
      LEFT JOIN
      (SELECT * FROM ledgerExtract) le
      ON 
        pb.pendingsourceid = le.ledger_source_sub_id 
        and pb.uuid = le.ledger_source_id
    ) 
  )
SELECT 
  startDateMonth,
  COUNT(user_id) as recordCount,
  COUNT(DISTINCT(uuid)) uuid,
  COUNT(ledger_source_sub_id) ledger_count,
  SUM(sales) as sales,
  SUM(cart_amount) as cart,
  SUM(reward_amount) as rewardsBolts,
  SUM(reward_amount)/250000 as rewardsUSD,
  SUM(reward_amount)/(250000*0.9) as expectedCommission,
  COUNT(DISTINCT(user_id)) as distinctAccounts,
  COUNT(DISTINCT(pendingsourceid)) as txnDistCount
FROM join1 
GROUP BY StartDateMonth 
ORDER BY StartDateMonth DESC
LIMIT 100;
