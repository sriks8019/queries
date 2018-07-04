--Q1. 1.	Identify marketing vehicle for all customers who joined in FY18
-- Assumption: all items purchased under same visit_date and visit_nbr will have same marketing vehicle
WITH
FY18_first_dotcom_customers AS
( 
	SELECT ugc_id, fp_dt_dotcom 
	FROM gcia_dotcom.omnichannel_base_partition_dotcom
	WHERE fp_dt_dotcom BETWEEN   '01-02-2017' AND '31-01-2018'
)

SELECT ugc_id, mkt_veh, 
FROM
(SELECT     os.ugc_id, os.visit_date, os.mkt_veh, ROW_NUMBER() OVER(PARTITION BY  os.ugc_id, os.visit_date ORDER BY os.visit_nbr) as purchase_number
FROM gcia_dotcom.omnichannel_sol_v3  os LEFT SEMI JOIN FY18_first_dotcom_customers fpc
ON os.ugc_id=fpc.ugc_id
AND os.visit_date=fpc.fp_dt_dotcom) T
WHERE T. purchase_number=1

--Q2. 2.	Identify Avg basket size, channel, platform split for all customers in FY18

WITH
FY18_customers AS
(
SELECT COALESCE(individual_id, ugc_id ) as cust_id, channel, platform_type, visit_date, visit_nbr
FROM gcia_dotcom.omnichannel_sol_v3
WHERE fiscal_year_nbr=2018 --visit_date BETWEEN   '01-02-2017' AND '31-01-2018'
)

SELECT cust_id, channel, platform, AVG(basket_size) as average_basket_size
FROM
(SELECT cust_id, channel, platform, visit_date, visit_nbr,   COUNT(qty)  as basket_size -- can use SUM instead of count, if basket size is num of items * qty
FROM FY18_customers
GROUP BY cust_id, channel, platform, visit_date, visit_nbr)T
GROUP BY cust_id, channel, platform

--Q3. 3.	Calculate churn rate (inactivity of 1 year) for all customers 
--a.	Identify the reason of higher churn rate
--b.	Evaluate churn rate at different customer life cycle
-- ASSUMPTION ugc_id is present for only DOTCOM customers
WITH 
inactive_dotcom_customers AS
( 
SELECT COUNT(ugc_id) as num_inactive_customers
FROM gcia_dotcom.omnichannel_fp_dt_base_final_v3_with_flags   -- or gcia_dotcom.omnichannel_base_partition_dotcom, but no filter on partition
WHERE DATEDIFF(CURRENT_DATE, lp_dt_dotcom) >=365
)

SELECT num_inactive_customers/total_customers as churn_rate
FROM inactive_dotcom_customers, (SELECT COUNT(ugc_id) AS total_customers FROM gcia_dotcom.omnichannel_fp_dt_base_final_v3_with_flags  )