--создаем таблицу для витрины если ее еще не существует
CREATE TABLE IF NOT EXISTS dzheparovdaniil.retention_mart (
	cohort_month date NULL,
	cohort_size int8 NULL,
	order_month date NULL,
	month_offset numeric NULL,
	customers int8 NULL,
	retention_rate numeric NULL,
  meta_timestamp timestamp default current_timestamp
);

--создаем временную таблицу, куда сохраняем нужные для анализа данные оплаченных заказов
CREATE TEMPORARY TABLE paid_orders AS
SELECT 
id,
order_date::date AS order_date,
date_trunc('month', order_date)::date AS order_month,
customer_id
FROM core.orders
WHERE order_type = 'paid'
;

--очищаем витрину от данных, чтобы сделать полную перезагрузку
TRUNCATE TABLE dzheparovdaniil.retention_mart
;

--пишем запрос для вставки данных с рассчитанным retention
--перечисляем в явном виде поля для вставки, в поле meta_timestamp запишется дата вставки данных в витрину
INSERT INTO dzheparovdaniil.retention_mart (cohort_month, cohort_size, order_month, month_offset, customers, retention_rate)
--определяем для каждого клиента первый месяц его покупки
WITH cohort AS (
SELECT 
customer_id,
min(order_month) AS cohort_month
FROM paid_orders
GROUP BY customer_id
), 
--определяем размер когорты = кол-во клиентов, купивших первый раз в данном месяце
cohort_size AS (
SELECT cohort_month,
count(DISTINCT customer_id) AS cohort_size
FROM cohort
GROUP BY cohort_month
)
--считаем для каждого последующего месяца сколько клиентов возвращались с последующими покупками и считаем долю от размера когорты
SELECT 
c.cohort_month,
s.cohort_size,
o.order_month,
EXTRACT(month FROM age(o.order_month, c.cohort_month)) AS month_offset,
count(DISTINCT o.customer_id) AS customers,
round(count(DISTINCT o.customer_id)::NUMERIC * 100 /s.cohort_size::NUMERIC, 2) AS  retention_rate
FROM paid_orders o
JOIN cohort c 
ON o.customer_id = c.customer_id
JOIN cohort_size s
ON s.cohort_month = c.cohort_month
GROUP BY 1,2,3,4
;
