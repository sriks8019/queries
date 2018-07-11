/*

Assume today as 31st March 2018 and fetch results
1. Tag the First Transaction of a User in a year for MDSE_l0 = 50000 keeping all data intact
2. Calculate the distribution of sales (total_auth_amt) and quantity (qty) across each marketing vehicle in this year and last year
3. calculate the number of new and Repet customers for Rollong 12 months (as of march 31st) in OG considering new customers as making their first transaction in OG and repeat as people having placed an order between rolling 13 - 24 months. (people having an order before rolling 24 months to be considered as new)
4. Repartition the table (can Create new table) on wm_week_nbr and MDSE_l0 for all store transactions within R10 months
5. Find the top 10 Categories within Dotcom which have the highest qty sold in the last 90 days
6. Count all orders having MDSE_l0 as 50000 within the last 27 days for dotcom
7. For all Dotcom customers, find their R12 month spend and their R13-24 month spend (tag new customers with "NEW*" in last year spend)
8. Bucket dotcom, og & store customers individually of last 180 days for every decile based on total_auth_amt 
9. For store customers, tag all customers in the last one year to the store that they most frequently visit
10. find the spent of all customers who were active for the last three years, 
for all three years as well as overall who are individually active for all three years and tag the top decile customers based on spent  overall value
11. Find the top sold subcat in the last 90 days and find out the subcategory which was also purchased 1 out of 10 times atleast with the top sold subcategory within a store
12. compare the Dotcom spent trend with store spent trend for the last month daywise and find out the days which had maximum orders
13. Find out the department having more inexpensive products


*/
--Q1. 1.	Tag the First Transaction of a User in a TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP()))
SELECT ugc_id, visit_date, group_order_nbr, 
			  CASE ROW_NUMBER() OVER (PARTITION BY ugc_id ORDER BY visit_date,group_order_nbr) WHEN 1 THEN 1 ELSE 0 END AS mdse_first_flag 
FROM gcia_dotcom.omnichannel_sol_v3 osv
WHERE channel IN ('DOTCOM')
AND visit_date BETWEEN '01-04-2017' AND '31-04-2018'
AND mdse_l0=50000
GROUP BY ugc_id, visit_date, group_order_nbr;

--2
SELECT mkt_veh,YEAR(visit_date), SUM(total_aut_amt), SUM(qty)
FROM gcia_dotcom.omnichannel_sol_v3
WHERE CHANNEL IN ('DOTCOM', 'OG')
AND visit_date BETWEEN '01-04-2016' AND '31-04-2018' -- fiscal_year_nbr IN (2017, 2018)
GROUP BY mkt_veh, YEAR(visit_date)

--Q3.
WITH first_time_customers AS
( SELECT ugc_id, fp_dt_dotcom 
	FROM gcia_dotcom.omnichannel_base_partition_og
	WHERE DATEDIFF(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP()), fp_dt_og ) BETWEEN 365 AND 365*2
)
SELECT ugc_id, group_order_nbr, CASE COUNT() OVER (PARTITION BY ugc_id) as num_of_transactions WHEN 1 THEN 'NEW' ELSE 'REPEAT' END as customer_type
LEFT SEMI JOIN first_time_customers
ON osv.ugc_id=fc.ugc_id
AND osv.CHANNEL='OG' 
WHERE DATEDIFF(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP()), visit_date ) <= 365*2
GROUP BY ugc_id, group_order_nbr 

--Q4
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions.pernode = 400;
CREATE TABLE transactions_partitioned_weeknbr_mdse
(
/* columns except */
)
PARTITIONED BY (wm_week_nbr, MDSE_l0 INT)
SELECT 
, wm_week_nbr, mdse_l0
FROM gcia_dotcom.omnichannel_sol_v3

--5 
SELECT * 
FROM
(SELECT cat,  RANK() OVER ( ORDER BY SUM(qty)) as cat_rank
FROM gcia_dotcom.omnichannel_sol_v3
WHERE CHANNEL IN ('DOTCOM')
AND DATE_DIFF(CURRENT_DATE,visit_date )<=90
GROUP BY cat) T
WHERE T.cat_rank BETWEEN 1 and 10

--6
SELECT group_order_nbr, COUNT(*) OVER () As total_orders
FROM gcia_dotcom.omnichannel_sol_v3
WHERE CHANNEL IN ('DOTCOM')
AND DATEDIFF(CURRENT_DATE,visit_date )<=27
AND mdse_l0=50000
GROUP BY group_order_nbr

--alternate 1
SELECT COUNT(distinct group_order_nbr)  as total_orders
FROM gcia_dotcom.omnichannel_sol_v3
WHERE CHANNEL IN ('DOTCOM')
AND DATEDIFF(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP())),visit_date )<=27
AND mdse_l0=50000;

--alternate 1
SELECT MAX(order_num) as total_orders
FROM
(SELECT DENSE_RANK OVER (ORDER BY group_order_nbr ) as order_num
FROM gcia_dotcom.omnichannel_sol_v3
WHERE CHANNEL IN ('DOTCOM')
AND mdse_l0=50000
AND DATEDIFF(CURRENT_DATE,visit_date )<=27) T
--Q7
SELECT ugc_id, year_num, month_num, SUM(SUM(total_auth_amount) ) OVER (PARTITION BY ugc_id, year_num ORDER BY year_num, month_num ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW )
FROM 
(SELECT  ugc_id,  MONTH(visit_date) as month_num, total_auth_amount, CASE WHEN DATEDIFF(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP()), visit_date ) <=365 THEN 1 ELSE 2 END as year_num
FROM gcia_dotcom.omnichannel_sol_v3
WHERE CHANNEL='DOTCOM'
AND DATEDIFF(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP()), visit_date ) <= 365*2
) A
GROUP BY year_num, month_num

--Q8
WITH last_180_days
AS
(
SELECT COALESCE(ugc_id,individual_id ) as cust_id, channel, total_auth_amount
FROM gcia_dotcom.omnichannel_sol_v3
WHERE DATEDIFF(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP()), visit_date))<=180
)

SELECT channel, cust_id,CASE WHEN NTILE(10) OVER(PARTITION BY channel ORDER BY  SUM(total_aut_amount))
FROM last_180_days
GROUP BY channel, cust_id
--Q9
WITH
preferred_store AS
(			
			SELECT  individual_id, store_nbr
			(SELECT  individual_id, store_nbr, visit_date, visit_nbr, 
			 ROW_NUMBER() OVER  ( partition by individual_id ORDER BY COUNT(CAST(visit_date as VARCHAR) +' '+CAST(visit_nbr AS VARCHAR)) DESC) AS visit_sale_preference
			FROM gcia_dotcom.omnichannel_sol_v3
			WHERE visit_date BETWEEN  '01-02-2017' AND TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP()))
			GROUP BY individual_id, store_nbr, visit_date, visit_nbr
			) A
			WHERE visit_sale_preference=1
)

SELECT s.*, p.store_nbr
FROM  preferred_store p RIGHT OUTER JOIN  gcia_dotcom.omnichannel_sol_v3 s
ON p.individual_id=s.indiviual_id;

--Q10
--assumption no conlficts between ugc and individual ids, years are calender years
SELECT cust_id, SUM(total_amt), CASE NTILE(10) OVER (ORDER SUM(total_amt)) WHEN 10 THEN 1 ELSE 0 END as top_decile                                                                                                                         --, SUM(SUM(total_auth_amount)) OVER (PARTITION BY cust_id)
FROM
(SELECT COALESCE(ugc_id, individual) as cust_id, YEAR(visit_date) as visit_year, SUM( total_auth_amount) as total_amt
FROM gcia_dotcom.omnichannel_sol_v3
WHERE YEAR(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP()))) -YEAR(visit_date) <=2
GROUP BY COALESCE(ugc_id, individual) , YEAR(visit_date)
) A
GROUP BY cust_id
HAVING COUNT(*)=3;


--Q11
SELECT subcat, CASE  WHEN percent_of_top_subcat_total=1 THEN 'TOP Sub Category' ELSE 'ATLEAST 1/10 of Top subcat'  END AS Subcat_sale_desc
FROM
(SELECT subcat, SUM(total_auth_amount) /First_value() OVER ( ORDER BY SUM(total_auth_amount) DESC)AS percent_of_top_subcat_total
FROM gcia_dotcom.omnichannel_sol_v3
WHERE DATEDIFF(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP()), visit_date))<=90
GROUP BY subcat) A
WHERE percent_of_top_subcat_total>=0.1

--Q12
SELECT * 
FROM 
((SELECT 'DOTCOM' as channel, visit_date,group_order_number, SUM(SUM(total_auth_amount)) OVER (PARTITION BY visit_date ) AS total_amount,  COUNT(group_order_number) OVER(PARTITION BY visit_date) AS num_orders
FROM gcia_dotcom.omnichannel_sol_v3
WHERE channel in ('DOTCOM')
AND DATEDIFF(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP()), visit_date))<=30
GROUP BY visit_date, group_order_number
)  UNION ALL
(SELECT 'STORE' AS channel, visit_date, channel,visit_number, SUM(total_auth_amount) OVER (PARTITION BY visit_date ) AS total_amount,  COUNT(visit_number) OVER(PARTITION BY visit_date) AS num_orders
FROM gcia_dotcom.omnichannel_sol_v3
WHERE channel in ('STORE')
AND DATEDIFF(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP()), visit_date))<=30
GROUP BY  visit_date,  visit_number) ) T
ORDER BY visit_date, channel;

-- alternatively a join can be used
SELECT A.visit_date, A.total_dotcom_amount, A.num_dotcom_orders, B.visit_date, B.total_store_amount, B.num_store_orders

FROM 
(SELECT 'DOTCOM' as channel, visit_date,group_order_number,SUM( SUM(total_auth_amount)) OVER (PARTITION BY visit_date ) AS total_dotcom_amount,  COUNT(group_order_number) OVER(PARTITION BY visit_date) AS num_dotcom_orders
FROM gcia_dotcom.omnichannel_sol_v3
WHERE channel in ('DOTCOM')
AND DATEDIFF(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP()), visit_date))<=30
GROUP BY visit_date, group_order_number
) A FULL OUTER JOIN
(SELECT 'STORE' AS channel, visit_date, channel,visit_number, SUM(total_auth_amount) OVER (PARTITION BY visit_date ) AS total_store_amount,  COUNT(visit_number) OVER(PARTITION BY visit_date) AS num_orders
FROM gcia_dotcom.omnichannel_sol_v3
WHERE channel in ('STORE')
AND DATEDIFF(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP()), visit_date))<=30
GROUP BY  visit_date,  visit_number) B
ON A.visit_date=B.visit_date
ORDER BY visit_date;


--Q13

WITH avg_price_rank as
(
SELECT dept,    RANK()  OVER (ORDER BY   SUM(total_auth_amount) /CAST(SUM(qty) AS FLOAT)) as avg_item_price_rank
FROM gcia_dotcom.omnichannel_slo_v3
WHERE channel IN ('DOTCOM')
GROUP BY dept)

SELECT dept
FROM avg_price
WHERE avg_price_rank =1;


