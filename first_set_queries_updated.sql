

/* 
1.    
In FY18, out of all customers who opted for pick up (PUT + S2S), how many (count and %age) have never placed a pick-up order of over $35?

a.     Some filters to help you:

1.     where visit_date between X and Y

2.     and channel = 'DOTCOM'

3.     and service_id in (8, 11) */

WITH pickup_cust_under_35

AS( SELECT  ugc_id

FROM 

WHERE                           fiscal_year_nbr=2018     --visit_date between '01-02-2017' AND '31-01-2018'         -- ( or FY18 dates)

                                      AND channel = 'DOTCOM'

                                       AND service_id in (8, 11)
GROUP BY ugc_id
HAVING      MAX( total_auth_amount) < =35

),

FY18_pickup_customers  -- all customers who opted for pickup in FY18

AS ( SELECT distinct ugc_id

FROM aanand2.omnichannel_sol_v1_random_ugc_1_percent

WHERE 

                                     fiscal_year_nbr=2018                                                           --visit_date between '01-02-2018' AND '31-01-2019'         -- ( or FY18 dates)

                                      AND channel = 'DOTCOM'

                                       AND service_id in (8, 11)

)

 

SELECT  COUNT(p.ugc_id)  as customer_under_35_count, COUNT(p.ugc_id)/ CAST(COUNT(f.ugc_id) AS FLOAT)  as percentage

FROM  FY18_customers f LEFT OUTER JOIN pickup_cust_under_35 p

ON f.ugc_id=p.ugc_id

 

 

/*2.     Cumulative revenue for 'DOTCOM' and 'OG' until end of each month of FY18 i.e. total revenue until end of Feb�17, until end of March�17� until end of Jan�18
*/
SELECT  channel,
        SUM(total_auth_amount)  OVER ( PARTITION BY channel ORDER BY MONTH(visit_date)) as cumulative_sum

FROM aanand2.omnichannel_sol_v1_random_ugc_1_percent

WHERE fiscal_year_nbr=2018

AND channel IN ('DOTCOM', 'OG')

 

 

/*3.     For each quarter of a fiscal year - what percentage of shoppers (dotcom only) shopping in a fiscal quarter, will shop again (repeat) in the following quarter?
 You'd have to look at Q1 for the next FY to get repeat rate for Q4
*/

WITH quarterly_customers

AS

( SELECT distinct ugc_id, DATEDIFF(quarter, '01-02-2017', visit_date)+1 as quarter     -- starting with Fiscal year 2018
   FROM aanand2.omnichannel_sol_v1_random_ugc_1_percent
 WHERE channel='DOTCOM'
 --AND DATEDIFF(quarter, '01-02-2017', visit_date)<5-- if interestested in strictly one year
)

SELECT qc2.quarter, COUNT(qc1.ugc_id) as count_repeat_customers, COUNT(qc1.ugc_id)/ CAST(COUNT(qc2.ugc_id) AS FLOAT) as repeat_percent

FROM quarterly_customers qc1 RIGHT OUTER JOIN quarterly_customers qc2

ON qc1.ugc_id= qc2.ugc_id

AND qc1.quarter=qc2.quarter+1
GROUP BY qc2.quarter
--WHERE qc2.quarter= x     --or (GROUP BY qc2.quarter) -- group by to get all quarters repeat customers


