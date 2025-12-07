--создаем временную таблицу для получения всех первых авторизаций пользователей.
--данных может быть много и операция DISTINCT ON достаточно тяжелая, поэтому лучше промежуточно сохранить результаты во временную таблицу
CREATE TEMP TABLE first_auth AS 
SELECT DISTINCT ON (customer_id)
customer_id,
auth_date::date AS auth_day
FROM raw.customer_auth
WHERE customer_id IS NOT NULL
ORDER BY customer_id, auth_date ASC
;

--создаем таблицу с актуальными данными для витрины
CREATE TABLE dzheparovdaniil.customers_base_mart_actual AS 
--считаем кол-во первый раз авторизованных клиентов по дням
WITH auth_customers AS (
SELECT 
auth_day,
count(customer_id) AS authorized_customers
FROM first_auth
GROUP BY 1
),
--считаем кол-во удаленных клиентов по дням
delete_customers AS (
SELECT 
customer_delete_dtm::date AS delete_day,
count(DISTINCT customer_id) AS deleted_customers
FROM raw.customer_delete
GROUP BY 1
),
--считаем кумулятивные суммы (нарастающие) авторизаций, удалений и разницу между ними с помощью оконных функций
--окно одинаковое, поэтому задать его лучше через конструкцию WINDOW и обращаться к окну по имени w
cum AS (
SELECT 
auth_day AS base_date,
authorized_customers,
COALESCE(deleted_customers, 0) AS deleted_customers,
sum(authorized_customers) OVER w AS authorized_customers_cum,
sum(COALESCE(deleted_customers, 0)) OVER w AS deleted_customers_cum,
sum(authorized_customers) OVER w - 
COALESCE(sum(deleted_customers) OVER w, 0) AS active_customers 
FROM auth_customers a
LEFT JOIN delete_customers d
ON a.auth_day = d.delete_day
WINDOW w AS (ORDER BY auth_day ASC)
)
SELECT 
base_date,
authorized_customers,
deleted_customers,
authorized_customers_cum,
deleted_customers_cum,
active_customers,
active_customers - lag(active_customers) OVER (ORDER BY base_date ASC) AS active_base_delta,
current_timestamp AS meta_timestamp
FROM cum
ORDER BY base_date ASC
;

--переименуем витрину данных в _old если она существует
ALTER TABLE IF EXISTS dzheparovdaniil.customers_base_mart
RENAME TO customers_base_mart_old
;

--переименуем таблицу с актуальными данными в целевое название витрины
ALTER TABLE dzheparovdaniil.customers_base_mart_actual
RENAME TO customers_base_mart
;

--удаляем таблицу со старой версией данных
DROP TABLE dzheparovdaniil.customers_base_mart_old
;
