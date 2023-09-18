--Milestone 3
--Task 1

ALTER TABLE orders_table 
ALTER COLUMN date_uuid SET DATA TYPE UUID USING date_uuid::uuid, 
ALTER COLUMN user_uuid SET DATA TYPE UUID USING user_uuid::uuid ,
ALTER COLUMN card_number SET DATA TYPE VARCHAR(120122) ,
ALTER COLUMN store_code SET DATA TYPE VARCHAR(120122) ,
ALTER COLUMN product_code SET DATA TYPE VARCHAR(120122), 
ALTER COLUMN product_quantity SET DATA TYPE SMALLINT; 

--Task 2

ALTER TABLE dim_users
ALTER COLUMN first_name SET DATA TYPE VARCHAR(255)
ALTER COLUMN last_name SET DATA TYPE VARCHAR(255)
ALTER COLUMN date_of_birth SET DATA TYPE DATE
ALTER COLUMN country_code SET DATA TYPE VARCHAR(15321)
ALTER COLUMN user_uuid  SET DATA TYPE UUID USING user_uuid::uuid 
ALTER COLUMN user_uuid  SET DATA TYPE UUID USING user_uuid::uuid 
ALTER COLUMN join_date  SET DATA TYPE DATE

--Task 3

ALTER TABLE dim_store_details
ALTER COLUMN longitude SET DATA TYPE FLOAT USING longitude::double precision
ALTER COLUMN locality SET DATA TYPE VARCHAR(255)
ALTER COLUMN store_code SET DATA TYPE VARCHAR(451)
ALTER COLUMN opening_date SET DATA TYPE DATE
ALTER COLUMN store_type SET DATA TYPE VARCHAR(255) 
ALTER COLUMN latitude SET DATA TYPE FLOAT USING latitude::double precision
ALTER COLUMN country_code SET DATA TYPE VARCHAR(451)
ALTER COLUMN continent SET DATA TYPE VARCHAR(255)
ALTER COLUMN staff_numbers SET DATA TYPE SMALLINT USING staff_numbers::smallint

--Task 4

SELECT REPLACE(product_price,'£','') AS product_price  FROM dim_products

UPDATE dim_products
SET product_price  = REPLACE(product_price, '£', '')

ALTER TABLE dim_products
ADD COLUMN weight_class VARCHAR(1853)

--Task 5

ALTER TABLE dim_products
RENAME removed to still_available

ALTER TABLE dim_products
ALTER COLUMN product_price SET DATA TYPE FLOAT USING product_price::double precision
ALTER COLUMN weight SET DATA TYPE FLOAT USING weight::double precision
ALTER COLUMN EAN SET DATA TYPE VARCHAR(1853)
ALTER COLUMN product_code SET DATA TYPE VARCHAR(1853)
ALTER COLUMN date_added SET DATA TYPE DATE USING date_added::date
ALTER COLUMN uuid SET DATA TYPE uuid USING uuid::uuid
ALTER COLUMN still_available SET DATA TYPE BOOL USING still_available::boolean
ALTER COLUMN weight_class SET DATA TYPE VARCHAR(1853)


--Task 6
ALTER TABLE dim_date_times
ALTER COLUMN month SET DATA TYPE VARCHAR(120122)
ALTER COLUMN day SET DATA TYPE VARCHAR(120122)
ALTER COLUMN year SET DATA TYPE VARCHAR(120122)
ALTER COLUMN time_period SET DATA TYPE VARCHAR(120122)
ALTER COLUMN date_uuid SET DATA TYPE uuid USING date_uuid::uuid

--Task 7

ALTER TABLE dim_card_details
ALTER COLUMN card_number SET DATA TYPE VARCHAR(15308)
ALTER COLUMN expiry_date SET DATA TYPE VARCHAR(15308)
ALTER COLUMN date_payment_confirmed SET DATA TYPE DATE

--Task 8
ALTER TABLE dim_products
ADD PRIMARY KEY (product_code)

ALTER TABLE dim_store_details
ADD PRIMARY KEY (store_code)

ALTER TABLE dim_card_details
ADD PRIMARY KEY (card_number)

ALTER TABLE dim_users
ADD PRIMARY KEY (user_uuid)

ALTER TABLE dim_date_times
ADD PRIMARY KEY (date_uuid)

--Task9 

--product_code
ALTER TABLE orders_table
ADD FOREIGN KEY (product_code) REFERENCES dim_products(product_code);

INSERT INTO dim_store_details
(store_code, staff_numbers, opening_date, store_type) 
VALUES ('WEB-1388012W', 325, '2010-06-12', 'Web Portal');

--store_code
DELETE FROM orders_table
WHERE store_code NOT IN (SELECT store_code FROM dim_store_details);

ALTER TABLE orders_table
ADD CONSTRAINT fk_store_orders
FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code);

--user_uuid
DELETE FROM orders_table
WHERE user_uuid NOT IN (SELECT user_uuid FROM dim_users);

ALTER TABLE orders_table
ADD CONSTRAINT fk_user_uuid_orders
FOREIGN KEY (user_uuid) REFERENCES dim_users(user_uuid);

--date_uuid
DELETE FROM orders_table
WHERE date_uuid NOT IN (SELECT date_uuid FROM dim_date_times);

ALTER TABLE orders_table
ADD CONSTRAINT fk_date_uuid_orders
FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid);

--card_number
DELETE FROM orders_table
WHERE card_number NOT IN (SELECT card_number FROM dim_card_details);

ALTER TABLE orders_table
ADD CONSTRAINT fk_card_number_orders
FOREIGN KEY (card_number) REFERENCES dim_card_details(card_number);


--Milestone 4

--Task 1
SELECT country_code, COUNT(*) as total_no_stores
FROM dim_store_details
GROUP BY country_code
ORDER BY total_no_stores DESC;

--Task 2
SELECT locality, COUNT(*) AS total_no_stores
FROM dim_store_details
GROUP BY locality
ORDER BY total_no_stores DESC

--Task 3
-- Check date times table with months present
--upload the table again
--create foreign key
--run the query

SELECT dim_date_times.month, 
SUM(orders_table.product_quantity * dim_products.product_price) AS total_sales
FROM dim_date_times
JOIN orders_table ON dim_date_times.date_uuid  = orders_table.date_uuid
JOIN dim_products ON orders_table.product_code  = dim_products.product_code
GROUP BY dim_date_times.month
ORDER BY total_sales DESC;

--Task 4
SELECT dim_store_details.locality
SUM(orders_table.product_quantity) AS product_quantity_count
SUM(orders_table.product_quantity * dim_products.product_price) AS total_sales
FROM dim_store_details
WHERE
   location = 'Web Portal'
   
SELECT dim_store_details.locality
SUM(orders_table.product_quantity) AS product_quantity_count
SUM(orders_table.product_quantity * dim_products.product_price) AS total_sales
FROM dim_store_details
WHERE
   location = 'Local'

--Task 5
SELECT 
    store_type,
     SUM(orders_table.product_quantity * dim_products.product_price) AS total_sales,
    (SUM(total_sales) / (SELECT SUM(total_sales) FROM sales)) * 100 AS percentage_total
FROM
    dim_store_details
GROUP BY
    store_type;

--Task 6
SELECT
    SUM(orders_table.product_quantity * dim_products.product_price) AS total_sales,
    EXTRACT(YEAR FROM order_date) AS year,
    EXTRACT(MONTH FROM order_date) AS month
FROM
    dim_date_times
GROUP BY
    year,
    month
ORDER BY
    total_sales DESC
LIMIT 10;

--Task 7
SELECT
    SUM(staff_numbers) AS total_staff_numbers,
    country_code
FROM
    dim_store_details
GROUP BY
    country_code;

--Task 8
SELECT
    SUM(orders_table.product_quantity * dim_products.product_price) AS total_sales,
    store_type,
    country_code
FROM
    dim_store_details
WHERE
    country_code = 'DE'  -- Filter for Germany
GROUP BY
    store_type,
    country_code
ORDER BY
    total_sales DESC;
