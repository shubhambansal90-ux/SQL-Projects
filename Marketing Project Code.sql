
CREATE TABLE online_sale(
CustomerID int,
Transaction_ID int,
Transaction_Date date,
Product_SKU	varchar(100),
Product_Description varchar (100),	
Product_Category varchar (100),
Quantity int,
Avg_Price numeric,
Delivery_Charges numeric,
Coupon_Status varchar (100)
);

drop table online_sale


select *
from online_sale
limit 100;


/** Top 10 spender in the 2019 **/ 

select CustomerID,
sum(quantity * avg_price) as revenue
from online_sale
group by CustomerID
order by revenue desc
limit 10;


/** Top product category in 2019 **/ 

select Product_Category,
sum(quantity * avg_price) as revenue
from online_sale
group by Product_Category
order by revenue desc;

/** Understanding how many customers acquired every month **/

with cte as (
select customerid,
transaction_id,
row_number() over (partition by customerid order by transaction_date, transaction_id) as rn,
Transaction_Date,
extract (month from transaction_date) as Month_num,
to_char(transaction_date, 'Month') as month_td
from online_sale
order by rn asc)

select Month_td, Month_num,
count(month_td) as New_customer
from cte
where rn = 1
group by month_td, month_num
order by month_num

/** Total customer and retaining customer month over month **/

with cte as (
select distinct customerid,
date_trunc ('month', transaction_date)::date as month_num,
to_char(transaction_date, 'Month') as Month_ch
from online_sale
order by month_num),

cte2 as (
select month_ch, month_num,
count(customerid) as Total_customer
from cte
group by month_ch, month_num
order by month_num)

select a.month_num, a.month_ch, 
c.Total_customer,
coalesce (count(b.customerid),0) as ret_cust
from cte a
join cte2 c on a.month_num = c.month_num
left outer join cte b
on a.customerid = b.customerid
and a.month_num = b.month_num + interval '1 month' 
group by a.month_num, a.month_ch, c.total_customer
order by a.month_num

/** How the revenues from existing/new customers on month on month basis **/

with cte as (
select *,
quantity * avg_price as revenue,
date_trunc('Month', transaction_date)::date as month_start
from online_sale),

cte2 as (select month_start, customerid, sum(revenue) as revenue
from cte
group by month_start, customerid
order by month_start)

select a.month_start,
count(a.customerid) as total_cust,
sum(a.revenue) as total_revenue,
count(b.customerid) as ret_cust,
sum(b.revenue) as ret_revenue
from cte2 a
left outer join cte2 b
on a.month_start = b.month_start + interval '1 month'
and a.customerid = b.customerid
group by a.month_start

;


/** How the discounts playing role in the revenues? **/

create table discount(
Month varchar (100),
Product_category varchar (100),
Coupon_Code varchar (100),
Discount_pct int
)

with cte as (
select product_category,
round(avg(discount_pct)/100,2) as discount_by_per,
1-round(avg(discount_pct)/100,2) as Rem
from discount
group by 1),

cte2 as (select a.product_category,
sum(a.quantity*a.avg_price) as revenue,
round(sum((a.quantity*a.avg_price) * b.rem),2) as rem_revenue
from online_sale a
join cte b
on a.product_category = b.product_category
group by a.product_category)

select *,
revenue - rem_revenue as lost_rev
from cte2
order by product_category;


/**Understand the trends/seasonality of sales by category, location, month **/

/** The top five months with the highest coupon usage **/

with cte as (select *,
date_trunc('month', transaction_date)::date as month_st
from online_sale)

select month_st, coupon_status,
count(*) as cpn_cnt
from cte 
where coupon_status = 'Used'
group by month_st, coupon_status
order by cpn_cnt desc
limit 5

/** quantity sold and revenue by category **/

select product_category,
sum(quantity) as sale,
round(sum(quantity)/ sum(sum(quantity)) over(),3) as sale_perc,
sum(quantity*avg_price) as revenue,
round(sum(quantity*avg_price)/sum(sum(quantity*avg_price)) over (),3)as revenue_prec
from online_sale
group by product_category
order by revenue desc

/** Months with highest sale**/

select 
to_char( transaction_date,'Month') as Mnth,
sum(quantity) as sale,
sum(quantity * avg_price) as revenue
from online_sale
group by to_char( transaction_date,'Month')
order by revenue desc


select 
to_char( transaction_date,'Month') as Mnth,
product_category,
sum(quantity) as sale
from online_sale
where to_char(transaction_date,'Month') = 'December '
group by to_char(transaction_date,'Month'), product_category
order by sale desc

/** sales by location **/

create table cus_info(
customerid int,
gender varchar(10),
location varchar(100),
tenure_month int
)

/** Sale and revenue by location **/

select b.location, 
sum(a.quantity) as sale,
sum(a.quantity * a.avg_price),
count(a.customerid) as total_order,
count(distinct(a.customerid)) as Distnct_cust 
from online_sale a
join cus_info b
on a.customerid = b.customerid
group by b.location
order by sale desc
;

/** understanding the buyer's gender by location**/

select location, gender,
count(gender) as cnt
from cus_info
group by location, gender
order by location, gender

/** category women are buying **/
/** Women are mostly buying office product but the business is generating money mostly through Nest-USA from women**/

with cte as (
select b.location,
a.product_category,
sum(a.quantity) as sale,
sum(a.quantity * a.avg_price) as revenue
from online_sale a
join cus_info b
on a.customerid = b.customerid
where b.gender = 'F'
group by b.location, a.product_category
order by location, revenue desc),

cte2 as (select *,
dense_rank() over (partition by location  order by location, revenue desc) as dr
from cte )

select location, product_category,
sale, revenue
from cte2
where dr=1

/** category men are buying **/
/** same for men as women**/

with cte as (
select b.location,
a.product_category,
sum(a.quantity) as sale,
sum(a.quantity * a.avg_price) as revenue
from online_sale a
join cus_info b
on a.customerid = b.customerid
where b.gender = 'M'
group by b.location, a.product_category
order by location, sale desc),

cte2 as (select *,
dense_rank() over (partition by location  order by location, sale desc) as dr
from cte )

select location, product_category,
sale, revenue
from cte2
where dr=1


/** Final revenue after discounts and marketing spends **/

create table marketing_spend (
date_ms date,
offline_spend int,
online_spend numeric
)

with cte as (select 
to_char(date_ms, 'Mon') as month_ch,
sum(offline_spend) as offline_spend,
sum(online_spend) as online_spend
from marketing_spend
group by to_char(date_ms, 'Mon') 
order by month_ch desc),

cte2 as (select 
month, round(avg(discount_pct),2) as avg_disc
from discount
group by month
order by month desc),

cte3 as (select
to_char(transaction_date, 'Mon') as month_ch,
sum(quantity * avg_price) as revenue,
sum(avg_price) as price
from online_sale 
group by to_char(transaction_date, 'Mon')
order by month_ch desc)

select a.month_ch,
a.revenue, b.offline_spend, b.online_spend, c.avg_disc,
round(a.revenue - b.offline_spend - b.online_spend - ((a.revenue*c.avg_disc)/100),2) as final_revenue
from cte3 a
join cte b on a.month_ch = b.month_ch
join cte2 c on a.month_ch = c.month
order by a.revenue desc
