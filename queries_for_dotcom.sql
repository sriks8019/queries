--Q1. 1.	Identify marketing vehicle for all customers who joined in FY18
-- Assumption: all items purchased under same visit_date and visit_nbr will have same marketing vehicle
WITH
FY18_first_dotcom_customers AS
( 
	SELECT ugc_id, fp_dt_dotcom 
	FROM gcia_dotcom.omnichannel_base_partition_dotcom
	WHERE fp_dt_dotcom BETWEEN   '01-02-2017' AND '31-01-2018'
)

SELECT ugc_id, mkt_veh
FROM
(SELECT     os.ugc_id, os.visit_date, os.mkt_veh, RANK() OVER( PARTITION BY oc.ugc_id  ORDER BY os.grp_order_nbr) as purchase_number
FROM (SELECT * FROM gcia_dotcom.omnichannel_sol_v3  WHERE channel='DOTCOM' AND visit_date BETWEEN   '01-02-2017' AND '31-01-2018' )os
LEFT SEMI JOIN FY18_first_dotcom_customers fpc
ON os.ugc_id=fpc.ugc_id
AND os.visit_date=fpc.fp_dt_dotcom) T
WHERE T. purchase_number=1

--Q2. 2.	Identify Avg basket size, channel, platform split for all customers in FY18
-- Query for dotcom customers
SELECT ugc_id, service_id,, platform, AVG(basket_size) as average_basket_size
FROM
(SELECT ugc_id, service_id, platform, grp_order_nbr,   SUM(qty)  as basket_size -- can use SUM instead of count, if basket size is num of items * qty
FROM gcia_dotcom.omnichannel_sol_v3
WHERE fiscal_year_nbr=2018 -- AND visit_date BETWEEN   '01-02-2017' AND '31-01-2018'
AND channel='DOTCOM'
AND service_id NOT IN (19,20)
GROUP BY ugc_id,service_id, platform, grp_order_nbr) T
GROUP BY ugc_id, service_id, platform


--Q3. 3.	Calculate churn rate (inactivity of 1 year) for all customers 
--a.	Identify the reason of higher churn rate
--b.	Evaluate churn rate at different customer life cycle
-- ASSUMPTION ugc_id is present for only DOTCOM customers
WITH 
inactive_dotcom_customers AS
( 
SELECT ugc_id 
FROM gcia_dotcom.omnichannel_base_partition_dotcom   -- or  gcia_dotcom.omnichannel_fp_dt_base_final_v3_with_flags   but no filter on partition
WHERE DATEDIFF(CURRENT_DATE, lp_dt_dotcom) >=365
)

SELECT COUNT(ugc_id)/total_customers as churn_rate as churn_rate
FROM inactive_dotcom_customers, (SELECT COUNT(ugc_id) AS total_customers FROM gcia_dotcom.omnichannel_base_partition_dotcom  )

--a and b
WITH 
inactive_dotcom_customers AS
( 
SELECT ugc_id 
FROM gcia_dotcom.omnichannel_base_partition_dotcom   -- or  gcia_dotcom.omnichannel_fp_dt_base_final_v3_with_flags   but no filter on partition
WHERE DATEDIFF(CURRENT_DATE, lp_dt_dotcom) >=365
),
inactive_dotcom_customer_purchases
(SELECT oc.ugc_id, oc.grp_order_nbr, SUM(oc.qty) as basket_size
FROM (SELECT * FROM gcia_dotcom.omnichannel_sol_v3  WHERE channel='DOTCOM' )oc LEFT SEMI JOIN inactive_dotcom_customers idc
 ON oc.ugc_id=idc.ugc.id
GROUP BY oc.ugc_d, oc.grp_order_nbr)

SELECT num_orders_before_churn, COUNT(ugc_id) AS num_of_customers
(SELECT ugc_id, COUNT(grp_order_number) AS num_orders_before_churn
FROM inactive_dotcom_customer_purchases
GROUP BY ugc_id
)
 GROUP BY num_orders
 
 --considering different churns not just the latest
WITH 
 churn_customers AS
 (
 SELECT ugc_id, visit_date AS churn_date, grp_order_number
 FROM
	 (SELECT ugc_id, visit_date, grp_order_number
	 LEAD(visit_date, 1 , CURRENT_DATE) OVER(PARTITION BY ugc_id ORDER BY visit_date) AS next_purchase 
	 FROM gcia_dotcom.omnichannel_sol_v3  
	 WHERE channel='DOTCOM' ) DT
 WHERE DATEDIFF(next_purchase , visit_date) >=365
 ),
 
orders_till_date AS
(SELECT ugc_id, visit_date, RANK() OVER (PARTITION BY ugc_id ORDER BY oc.visit_date, oc.grp_order_nbr ) AS num_orders
FROM
 (SELECT * FROM gcia_dotcom.omnichannel_sol_v3  WHERE channel='DOTCOM' )oc 
)
SELECT otd.ugc_id, otc.visit_date, num_orders
FROM orders_till_date otd JOIN churn_customers cc
 ON otd.ugc_id=cc.ugc_id
 AND otd.visit_date=cc.visit_date
AND otd.grp_order_nbr=cc.grp_order_nbr

