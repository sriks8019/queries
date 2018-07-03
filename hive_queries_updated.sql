--Question 1 preferred stores 
-- q1. a) not accounting for ties between stores for a customer
WITH
preferred_store AS
(
	SELECT vt_individual_customer_id, store_nbr, ROW_NUMBER() OVER (partition by vt_individual_customer_id ORDER BY num_visits DESC ) as preference
		FROM
			(SELECT  vt_individual_customer_id, store_nbr, COUNT(distinct CAST(visit_date as VARCHAR) +' '+CAST(visit_nbr AS VARCHAR)) as num_visits
			FROM cust_snapshot_july18
			WHERE visit_date BETWEEN  '01-02-2017' AND '31-01-2018'
			GROUP BY vt_individual_customer_id, store_nbr
			) A
	
)

SELECT p.store_nbr, COUNT(p.vt_individual_customer_id) OVER (PARTITION BY p.store_nbr)/ CAST(PSC.total_assigned_customers AS FLOAT)  as percentage
FROM preferred_store p, (SELECT  COUNT(vt_individual_customer_id)  as total_assigned_customers FROM preferred_store ) PSC
WHERE p.preference=1
GROUP BY store_nbr

-- q1. b) accounting for ties between stores for a customer, choosing the one with highest sales
-- assuming sales as the total dollar amount of sales
WITH
preferred_store AS
(
	SELECT vt_individual_customer_id, store_nbr
	   FROM
		(SELECT vt_individual_customer_id, store_nbr,  ROW_NUMBER() OVER (partition by vt_individual_customer_id ORDER BY num_visits  DESC, total_sales DESC) AS visit_sale_preference
		FROM
			(SELECT  vt_individual_customer_id, store_nbr, COUNT(distinct CAST(visit_date as VARCHAR) +' '+CAST(visit_nbr AS VARCHAR)) as num_visits, SUM(retail_price) as total_sales
			FROM cust_snapshot_july18
			WHERE visit_date BETWEEN  '01-02-2017' AND '31-01-2018'
			GROUP BY vt_individual_customer_id, store_nbr
			) A
	) B
WHERE visit_sale_preference=1 
)
SELECT store_nbr, COUNT(vt_individual_customer_id) OVER (PARTITION BY store_nbr)/ CAST(PSC.total_assigned_customers AS FLOAT) as percentage
FROM preferred_store , (SELECT  COUNT(vt_individual_customer_id)  as total_assigned_customers FROM preferred_store ) PSC
GROUP BY store_nbr

--q2
--Assuming that the stores we are interested in are the stores from cust_snapshot_july18
--steps to load the excel sheet
CREATE TABLE FY17_stores
( store_nbr SMALL_INT,
  new_to_profile VARCHAR(10),
  open_all_days   SMALLINT,
  total_households  INT
)
ROW FORMAT DELIMITED
 FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '/home/report_FY17.csv' OVERWRITE INTO TABLE excel_sheet_stores ;

--a) query to create FY_18 report
WITH
open_365 AS
(
	SELECT store_nbr, COUNT(distinct visit_date) as days_open
	FROM cust_snapshot_july18
	WHERE visit_date BETWEEN  '01-02-2017' AND '31-01-2018'
	GROUP BY store_nbr
)

SELECT o.store_nbr, CASE  WHEN es.store_nbr IS NULL THEN 'NEW' ELSE 'EXISTING'    AS store_status, CASE  WHEN o.days_open=365 THEN 1 ELSE 0  AS open_all_days

FROM open_365 o LEFT OUTER JOIN report_FY17 es
ON qs.store_nbr=es.store_nbr

--- the question does not mention about how to create the last column 'total households' in the report
--assuming the total households can be calculated from total number of vt_individual_customer_id's
--b) FY_18 report with 'total households impacted' column
WITH
open_365 AS
(
	SELECT store_nbr, COUNT(distinct visit_date) as days_open, COUNT(distinct vt_individual_customer_id) as total_households_impacted
	FROM cust_snapshot_july18
	WHERE visit_date BETWEEN  '01-02-2017' AND '31-01-2018'
	GROUP BY store_nbr
)

SELECT	 o.store_nbr, 
				CASE  WHEN es.store_nbr IS NULL THEN 'NEW' ELSE 'EXISTING'    AS store_status, 
				CASE  WHEN o.days_open=365 THEN 1 ELSE 0  AS open_all_days, o.total_households_impacted
FROM open_365 o LEFT OUTER JOIN report_FY17 es
ON qs.store_nbr=es.store_nbr