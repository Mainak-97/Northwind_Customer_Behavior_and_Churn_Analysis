use northwind;
show tables;

/* Objective 1:- Order Frequency Analysis: 
Investigate the frequency of customer orders over time and compute the average time between orders for each customer.  */
with OrderFrequency as (
	select
		customer_id,
        order_date,
        lag(order_date) over (partition by customer_id order by order_date) as prev_order_date,
        datediff(order_date, lag(order_date) over (partition by customer_id order by order_date)) as time_gap
	from orders
)
select
	customer_id,
    avg(time_gap) as average_time_gap
from OrderFrequency
group by customer_id
order by average_time_gap desc;

/* Objective 2:- Customer Segmentation: 
Segment customers based on their purchasing frequency (Frequent, Regular, and Infrequent Buyers) and analyze the average time gap between their orders. */
with OrderFrequency as (
	select
		customer_id,
        order_date,
        lag(order_date) over (partition by customer_id order by order_date) as prev_order_date,
        datediff(order_date, lag(order_date) over (partition by customer_id order by order_date)) as time_gap
	from orders
),
CustomerSegments as (
	select
		customer_id,
        case
			when avg(time_gap) <= 30 then "Frequent Buyer"
            when avg(time_gap) <= 90 then "Regular Buyer"
			else "Infrequent Buyer"
		end as segment
	from OrderFrequency
    group by customer_id
)
select
	segment,
    avg(time_gap) as average_time_gap
from OrderFrequency o
inner join CustomerSegments c on o.customer_id = c.customer_id
group by segment;

/* Objective 3:- Churn Analysis: 
Determine the average order value for customers who have churned vs. those who have remained active. */
with ChurnStatus as (
	select
		c.id,
        case
			when o.customer_id is not null then "Non-Churning"
            else "Churning"
		end as churn_status
	from customers c 
    left join orders o on c.id = o.customer_id
),
OrderValue as (
	select
		o.customer_id,
        sum(od.quantity * od.unit_price) as order_value
	from orders o
    inner join order_details od on o.id = od.order_id
    group by o.customer_id
)
select
	cs.churn_status,
    avg(ov.order_value) as average_order_value
from ChurnStatus cs
inner join OrderValue ov on cs.id = ov.customer_id
group by cs.churn_status;

/* Objective 4:- Order Value Distribution: 
Categorize customers based on their total order value (Low, Medium, High) and identify the distribution of these categories across the customer base. */
with OrderValueDistribution as (
	select
		c.id,
        sum(od.quantity * od.unit_price) as order_value
	from customers c 
    left join orders o on c.id = o.customer_id
    left join order_details od on o.id = od.order_id
    group by c.id
)
select
	case 
		when order_value <= 1000 then "Low Order Value"
		when order_value <= 5000 then "Medium Order value"
		else "High Order Value"
	end as order_value_category,
    count(id) as customer_count
from OrderValueDistribution
group by order_value_category
order by customer_count desc;

/* Objective 5:- Churn Impact on Product Categories: 
Identify product categories that are frequently purchased before and after a churn event. */
with Churned_Customers as (
	select distinct customer_id
    from orders
    where status_id = 3
)
select p.category,
	count(case when o.customer_id in (select customer_id from Churned_Customers) then o.id end) as churned_count,
    count(case when o.customer_id not in (select customer_id from Churned_Customers) then o.id end) as active_count
from orders o 
inner join order_details od on o.id = od.order_id
inner join products p on od.product_id = p.id
group by p.category
order by churned_count desc;

/* Objective 6:- Churn by Region: 
Investigate churn rates across different customer locations (cities, states, and countries) to identify regions with high churn. */
with Churned_Customers as (
	select distinct customer_id
    from orders
    where status_id in (
		select id from orders_status where status_name = "Closed" or status_name = "Shipped"
	)
),
Customer_Locations as (
	select c.id,
		   c.company,
           c.city,
           c.state_province,
           c.country_region,
           case when cc.customer_id is not null then "Churned" else "Active" end as customer_status
	from customers c 
    left join Churned_Customers cc on c.id = cc.customer_id
)
select 
	country_region,
    state_province,
    city,
    count(case when customer_status = "Churned" then 1 end) as churned_count,
    count(case when customer_status = "Active" then 1 end) as active_count,
    round((count(case when customer_status = "Churned" then 1 end) * 100.0) / count(*), 2) as churned_rate
from Customer_Locations
group by country_region, state_province, city
order by churned_rate desc;

/* Objective 7:- Purchase Behavior by Region: 
Examine the correlation between customer location and purchase behavior to assess any geographical influences on buying patterns. */
select
	c.country_region,
    c.state_province,
    c.city,
    count(o.id) as total_orders,
    sum(od.quantity) as total_quantity,
    sum(od.quantity * od.unit_price) as total_revenue
from 
	customers c
inner join
	orders o on c.id = o.customer_id
inner join
	order_details od on o.id = od.order_id
group by
	c.country_region, c.state_province, c.city
order by
	c.country_region, c.state_province, c.city;

/* Objective 8:- Customer Risk Scoring: 
Assign a risk score to each customer based on their order frequency, spending behavior, and product category preferences. */
select
	c.id as customer_id,
    c.company as company_name,
    count(o.id) as total_orders,
    sum(od.quantity * od.unit_price) as total_spent,
	case
		when count(o.id) >= 7 and sum(od.quantity * od.unit_price) >= 1000 then "Low Risk"
        when count(o.id) between 4 and 7 and sum(od.quantity * od.unit_price) between 500 and 999 then "Medium Risk"
        else "High Risk"
	end as Risk_Category
from
	customers c
left join
	orders o on c.id = o.customer_id
left join 
	order_details od on o.id = od.order_id
group by
	c.id, c.company
order by
	total_orders desc, total_spent desc;

/* Objective 9:- Order Frequency in the Last 6 Months: 
Measure the number of orders placed by customers in the last 6 months to identify any recent changes in order behavior. */
select
	c.id as customer_id,
    count(distinct o.id) as total_orders_last_6_months
from
	customers c 
left join
	orders o on c.id = o.customer_id
where
	o.order_date >= date_sub((select max(order_date) from orders), interval 6 month)
group by
	c.id;

/* Objective 10: Decreasing Order Frequency: 
Identify customers with a decrease in order frequency over the last 6 months compared to the average for all customers. */
select 
	c.id as customer_id,
    c.company,
    count(distinct o.id) as total_orders
from
	customers c
inner join
	orders o on c.id = o.customer_id
where
	o.order_date >= date_sub((select max(order_date) from orders), interval 6 month)
group by
	c.id, c.company
having
	count(distinct o.id) < (select avg(order_count) from (
								select
									c.id as customer_id,
                                    count(distinct o.id) as order_count
								from
									customers c
								inner join
									orders o on c.id = o.customer_id
								where
									o.order_date >= date_sub((select max(order_date) from orders), interval 6 month)
								group by
									c.id
							) as order_counts);

/* Objective 11: Customer Lifetime Value (CLTV): 
Calculate the Customer Lifetime Value (CLTV) for each customer based on their spending, order frequency, and other relevant factors to prioritize retention efforts. */
select
	c.id as customer_id,
    round(sum(od.quantity * od.unit_price * (1 - od.discount)) - sum(o.shipping_fee + o.taxes),2) as CLTV
from
	customers c
inner join
	orders o on c.id = o.customer_id
inner join
	order_details od on o.id = od.order_id
group by
	c.id
order by
	CLTV desc;
    
/* Objective 12:- Customer Acquisition Trends:
Analyze trends in customer acquisition over time, such as how many new customers are added each month. */
select
    year(o.order_date) as year_acquired,
    month(o.order_date) as month_acquired,
    count(distinct o.customer_id) as new_customers,
    sum(od.quantity * od.unit_price) as total_sales
from
    orders o
inner join
    order_details od on o.id = od.order_id
group by
    year(o.order_date),
    month(o.order_date)
order by
    year_acquired, month_acquired;
