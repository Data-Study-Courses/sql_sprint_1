CREATE VIEW dzheparovdaniil.rfm_mart AS 
SELECT
customer_id,
count(DISTINCT id) AS orders_count,
count(DISTINCT id) FILTER (WHERE order_type='paid') AS orders_paid_count,
extract(day from (now() - max(order_date))) as days_last_order,
min(order_date) FILTER (WHERE order_type='paid') AS first_paid_order_date, 
sum(amount * quantity * (1 - (coalesce(discount::numeric,0) / 100))) AS orders_revenue,
sum(amount * quantity * (1 - (coalesce(discount::numeric,0) / 100))) FILTER (WHERE order_type = 'paid') AS orders_paid_revenue,
sum(amount * quantity * (1 - (coalesce(discount::numeric,0) / 100))) FILTER (WHERE order_type = 'paid') / count(DISTINCT id) FILTER (WHERE order_type='paid') AS avg_bill
FROM core.orders
GROUP BY customer_id
