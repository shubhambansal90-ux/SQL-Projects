-- Creating a table and importing csv 

create table cohort(

customerid int,
created_date date,
canceled_date date,
subscription_cost int,
subscription_interval varchar(100),
was_subscription_paid varchar(3)
)

-- Looking at the table/data we imported

select *
from cohort;

-- Trying to understand if we have returning customers

with cte as (
select customerid,
row_number() over (partition by customerid order by created_date asc) as Dup_id
from cohort)

select count(*) as returning_customers 
from cte 
where dup_id > 1
;

select *
from cohort
order by customerid;

-- Looking at different subscription cost

select subscription_cost, count(*)
from cohort
group by subscription_cost;

-- Indentifying if we have different offering (Like monthly, quarterly)

select subscription_interval, count(*)
from cohort
group by subscription_interval;

-- Finding out the LTV of the business

WITH CTE AS (
SELECT *,
CEIL((
CASE
WHEN canceled_date IS NULL
THEN (MAX(created_date) OVER () - created_date)
ELSE (canceled_date - created_date)
END)::NUMERIC/30.0) AS date_diff
FROM cohort)

select round(avg(subscription_cost * date_diff),2) as LTV
from cte
where was_subscription_paid = 'Yes'



