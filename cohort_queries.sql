/*
Questions:
1.	Look at all customers who have shopped in a year
2.	Need cohort counts of when a customer shops for the first time in a year i.e. if person X’s first order was in March then we’d track person X against March cohort
3.	What percentage of March cohort reaches 2 online orders and when?
*/
--Queries:
/*
Assumptions:
1. Transactions are sucessful orders/purchases
2. visit_date is a datetimestamp
*/
--1.
SELECT distinct customer_id
FROM transactions
WHERE visit_date BETWEEN ’01-02-2018’ AND ’31-01-2019’; --Fiscal year

--2.	
SELECT cc.visit_month as cohort, COUNT(cc.customer_id) as cohort_count
FROM
(SELECT customer_id, MONTH(visit_date) as visit_month,  ROW_NUMBER() OVER ( PARTITION BY customer_id ORDER BY visit_date ) as transaction_number
FROM transactions
WHERE  Fiscal_year='FY18'                    -- or YEAR(visit_date)=2018  -- year of interest or date_range
) cc
WHERE cc.transaction_number=1
GROUP BY cc.visit_month;

--3.	
WITH customer_wise_transactions AS
(SELECT customer_id, MONTH(visit_date) as visit_month,  ROW_NUMBER() OVER ( PARTITION BY customer_id ORDER BY visit_date ) as transaction_number
FROM transactions
WHERE fiscal_year='FY18' --YEAR(visit_date)=2018  -- year of interest or date range
)
march_cohort AS
( SELECT customer_id 
FROM customer_wise_transactions

WHERE transaction_number=1
AND visit_month=3
)
SELECT 	t.visit_month,
	COUNT(t.customer_id)   as second_purchase_count, 
 	COUNT(t.customer_id)  / (SELECT CAST(COUNT(mb.customer_id) AS FLOAT)) FROM march_cohort mb)  * 100 as month_wise_second_purchase_percentage
FROM customer_wise_transactions t LEFT SEMI JOIN march_cohort mc -- LEFT SEMI JOIN for HIVE, for SQL: IN(SELECT customer_id FROM march_cohort)
	ON t.customer_id=mc.customer_id
WHERE t.transaction_number=2
GROUP BY visit_month;

/*
--if interested in total percentage and cumulative percentages

SELECT visit_month as [month],
           month_wise_second_purchase_percentage ,
       SUM(month_wise_second_purchase_percentage) OVER ( ORDER BY visit_month ) AS cumulative_second_purchase_percentage
FROM
( query 3
)
*/
