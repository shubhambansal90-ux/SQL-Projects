-- Creating the table framework

create table marketing (
Campaign_ID int,
Company varchar (100),
Campaign_Type varchar (100),
Target_Audience varchar (100),
Duration varchar (100),
Channel_Used varchar (100),
Conversion_Rate numeric,
Acquisition_Cost varchar (100),
ROI numeric,
Location varchar (100),
Language varchar (100), 
Clicks int,
Impressions int,
Engagement_Score int,
Customer_Segment varchar (100),
Date date
)

-- Having a look at the data  
  
select *
from marketing
limit 100
;

/** ETL **/
/** Changing acquisition cost from string to decimal**/

ALTER TABLE marketing
ADD aqc_cost DECIMAL(10,2),
ADD duration_num int;

update marketing 
set aqc_cost = cast(replace(replace(acquisition_cost,'$',''),',','') as numeric),
duration_num = cast(replace(duration,' days','') as int)

-- company level analysis

select company, count(*) as campaigns_ran,
round((sum(aqc_cost))/1000000,1) as aqc_cost_Mil,
round(avg(conversion_rate),4) as avg_cnv_rate,
round(avg(roi),2) as avg_roi
from marketing
group by company
order by company, campaigns_ran desc

-- Campaign level analysis

select campaign_type, count(*) Total_campgn_ran,
sum(impressions) as total_imp,
sum(Clicks) as total_clicks,
round(((sum(aqc_cost))/1000000),2) as aqc_cost_mil,
round(((sum(aqc_cost)/sum(impressions))*1000),2) as cpm,
round((sum(clicks)/sum(impressions)::decimal)*100,2) as ctr_perc,
round(avg(engagement_score),2) as avg_eng_score,
round(avg(conversion_rate),4) as avg_cnv_rate,
round(avg(roi),2) as avg_roi
from marketing
group by campaign_type;

-- Best Campaign for each age group in terms of ROI

with cte as (select target_audience, campaign_type, count(*) Total_campgn_ran, 
sum(impressions) as total_imp,
sum(Clicks) as total_clicks,
round(((sum(aqc_cost))/1000000),2) as aqc_cost_mil,
round(((sum(aqc_cost)/sum(impressions))*1000),2) as cpm,
round((sum(clicks)/sum(impressions)::decimal)*100,2) as ctr_perc,
round(avg(engagement_score),2) as avg_eng_score,
round(avg(conversion_rate),4) as avg_cnv_rate,
round(avg(roi),2) as avg_roi
from marketing
group by target_audience,campaign_type
order by target_audience, avg_roi desc),

cte2 as (select *,
row_number() over (partition by target_audience order by target_audience, avg_roi desc) as rn
from cte)

select *
from cte2
where rn = 1

-- Best campaign for each company in terms of ROI 

with cte as (select company, campaign_type, target_audience, count(*) Total_campgn_ran, 
sum(impressions) as total_imp,
sum(Clicks) as total_clicks,
round(((sum(aqc_cost))/1000000),2) as aqc_cost_mil,
round(((sum(aqc_cost)/sum(impressions))*1000),2) as cpm,
round((sum(clicks)/sum(impressions)::decimal)*100,2) as ctr_perc,
round(avg(engagement_score),2) as avg_eng_score,
round(avg(conversion_rate),4) as avg_cnv_rate,
round(avg(roi),2) as avg_roi
from marketing
group by company, campaign_type, target_audience
order by company, avg_roi desc),

cte2 as (select *,
row_number() over (partition by company order by company, avg_roi desc) as rn
from cte)

select *
from cte2
where rn = 1

-- For each company and traget audience which campaign works better in terms of ROI

with cte as (select company, target_audience, campaign_type, count(*) cnt_camp,
sum(impressions) as sm_imp,
sum(aqc_cost) as aqc_cost,
round((sum(aqc_cost)/sum(impressions)*1000),2) as cpm,
round(avg(engagement_score),2) as eng_sc,
round(avg(conversion_rate),4) as cnv_rt,
round(avg(roi),2) as avg_roi
from marketing
group by company, target_audience, campaign_type
order by company, target_audience, avg_roi desc),

cte2 as (select *,
row_number() over (partition by company,target_audience order by company,target_audience, avg_roi desc) as rn
from cte)

select *
from cte2
where rn = 1

-- Best Segment for each company in terms of ROI

with cte as (
select company, customer_segment, campaign_type,
round(avg(roi),2) as avg_roi
from marketing
group by company, customer_segment, campaign_type
order by company, avg_roi desc),

cte2 as (select *,
row_number() over (partition by company order by avg_roi desc) as rn
from cte)

select *
from cte2
where rn = 1


-- Best performing campaign_type for each customer segment in terms of ROI

with cte as (
select customer_segment, campaign_type,
round(avg(roi),2) as avg_roi
from marketing
group by customer_segment, campaign_type
order by customer_segment,avg_roi desc),

cte2 as (select *,
row_number() over (partition by customer_segment order by avg_roi desc) as rn
from cte)

select *
from cte2
where rn = 1
