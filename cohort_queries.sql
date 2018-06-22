/*
Questions:
1.	Look at all customers who have shopped in a year
2.	Need cohort counts of when a customer shops for the first time in a year i.e. if person X’s first order was in March then we’d track person X against March cohort
3.	What percentage of March cohort reaches 2 online orders and when?
*/
--Queries:
--1.
SELECT distinct customer_id
FROM transactions
WHERE visit_date BETWEEN ’01-01-2018’ AND ’31-12-2018’;

--2.	
SELECT visit_month as cohort, COUNT(customer_id) as cohort_count
FROM
(SELECT customer_id, MONTH(visit_date) as visit_month,  ROW_NUMBER() OVER ( PARTITION BY customer_id ORDER BY visit_date ) as transaction_number
FROM transactions
WHERE YEAR(visit_date)=2018  -- year of interest)
WHERE transaction_number=1
GROUP BY visit_month;

--3.	
WITH march_cohort AS
( SELECT customer_id 
FROM
(SELECT customer_id, MONTH(visit_date) as visit_month,  ROW_NUMBER() OVER ( PARTITION BY customer_id ORDER BY visit_date ) as transaction_number
FROM transactions
WHERE YEAR(visit_date)=2018  -- year of interest
)
WHERE transaction_number=1
AND visit_month=3
)
SELECT visit_month as [month],
           month_wise_second_purchase_percentage ,
       SUM(month_wise_second_purchase_percentage) OVER ( ORDER BY visit_month ) AS cumulative_second_purchase_percentage
FROM
(SELECT visit_month, COUNT(customer_id)   as second_purchase_count,  COUNT(customer_id)  / (SELECT CAST(COUNT(mb.customer_id) AS FLOAT)) FROM march_cohort mb)  * 100 as month_wise_second_purchase_percentage
FROM
(SELECT t.customer_id, MONTH(t.visit_date) as visit_month, ROW_NUMBER() OVER ( PARTITION BY t.customer_id ORDER BY t.visit_date ) as transaction_number
FROM transactions t LEFT SEMI JOIN march_cohort mc
ON t.customer_id=mc.customer_id)
             WHERE transaction_number=2
              GROUP BY visit_month);
