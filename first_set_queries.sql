

/* 
1.    
In FY18, out of all customers who opted for pick up (PUT + S2S), how many (count and %age) have never placed a pick-up order of over $35?

a.     Some filters to help you:

1.     where visit_date between X and Y

2.     and channel = 'DOTCOM'

3.     and service_id in (8, 11) */

WITH pickup_cust_under_35

AS( SELECT  customer_id

FROM transactions

WHERE 

                                      visit_date between '01-01-2018' AND '31-12-2018'         -- ( or FY18 dates)

                                      AND channel = 'DOTCOM'

                                       AND service_id in (8, 11)
GROUP BY customer_id
HAVING      MAX( amount) < =35

),

FY18_pickup_customers  -- all customers who opted for pickup in FY18

AS ( SELECT distinct customer_id

FROM table(transactions)

WHERE visit

                                      visit_date between ’01-01-2018’ AND ’31-12-2018’           -- ( or FY18 dates)

                                      AND channel = 'DOTCOM'

                                       AND service_id in (8, 11)

)

 

SELECT  COUNT(p.customer_id)  as customer_under_35_count, COUNT(p.customer_id)/ CAST(COUNT(f.customer_id) AS FLOAT)  as percentage

FROM  FY18_customers f LEFT OUTER JOIN pickup_cust_under_35 p

ON f.customer_id=p.customer_id

 

 

/*2.     Cumulative revenue for “DOTCOM” and “OG” until end of each month of FY18 i.e. total revenue until end of Feb’17, until end of March’17… until end of Jan’18
*/
SELECT  channel,
        SUM(amount) as cumulative_sum OVER ( PARTITION BY channel ORDER BY MONTH(order_date))

FROM transactions

WHERE transaction_date BETWEEN  ’01-01-2018’ AND ’31-12-2018’  

AND channel IN (“DOTCOM”, “OG”)

--highlighted part is optional if both of the channels can be combined

       

 

 

/*3.     For each quarter of a fiscal year - what percentage of shoppers (dotcom only) shopping in a fiscal quarter, will shop again (repeat) in the following quarter? You’d have to look at Q1 for the next FY to get repeat rate for Q4
*/
WITH quarterly_customers

AS

( SELECT distinct customer_id, DATEDIFF(quarter, ’01-01-2018’, order_date)+1 as quarter     -- date could be date of interest

   FROM transactions

 WHERE channel=’DOTCOM’

)

 

SELECT COUNT(qc1.customer_id) as count_repeat_customers, COUNT(qc1.customer_id)/ CAST(COUNT(qc2.customer_id) AS FLOAT) as repeat_percent

FROM quarterly_customers qc1 RIGHT OUTER JOIN quarterly_customers qc2

ON qc1.customer_id= qc2.customer_id

AND qc1.quarter=qc2.quarter+1

WHERE qc2.quarter= x     --or (GROUP BY qc2.quarter) -- group by to get all quarters repeat customers
