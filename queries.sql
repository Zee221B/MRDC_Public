#Task 1

ALTER TABLE orders_table 
ALTER COLUMN date_uuid SET DATA TYPE UUID USING date_uuid::uuid 

ALTER TABLE orders_table 
ALTER COLUMN user_uuid SET DATA TYPE UUID USING user_uuid::uuid 

ALTER TABLE orders_table 
ALTER COLUMN card_number SET DATA TYPE VARCHAR(120122) 

ALTER TABLE orders_table 
ALTER COLUMN store_code SET DATA TYPE VARCHAR(120122) 

ALTER TABLE orders_table 
ALTER COLUMN product_code SET DATA TYPE VARCHAR(120122) 

ALTER TABLE orders_table 
ALTER COLUMN product_quantity SET DATA TYPE SMALLINT 

#Task 2

ALTER TABLE dim_users
ALTER COLUMN first_name SET DATA TYPE VARCHAR(255)

ALTER TABLE dim_users
ALTER COLUMN last_name SET DATA TYPE VARCHAR(255)

ALTER TABLE dim_users
ALTER COLUMN date_of_birth SET DATA TYPE DATE

ALTER TABLE dim_users
ALTER COLUMN country_code SET DATA TYPE VARCHAR(15321)

ALTER TABLE dim_users
ALTER COLUMN user_uuid  SET DATA TYPE UUID USING user_uuid::uuid 

ALTER TABLE dim_users
ALTER COLUMN user_uuid  SET DATA TYPE UUID USING user_uuid::uuid 

ALTER TABLE dim_users
ALTER COLUMN join_date  SET DATA TYPE DATE

#Task 3

ALTER TABLE dim_store_details
ALTER COLUMN longitude SET DATA TYPE FLOAT USING longitude::double precision

ALTER TABLE dim_store_details
ALTER COLUMN locality SET DATA TYPE VARCHAR(255)

ALTER TABLE dim_store_details
ALTER COLUMN store_code SET DATA TYPE VARCHAR(451)

ALTER TABLE dim_store_details
ALTER COLUMN opening_date SET DATA TYPE DATE

ALTER TABLE dim_store_details
ALTER COLUMN store_type SET DATA TYPE VARCHAR(255) 

ALTER TABLE dim_store_details
ALTER COLUMN latitude SET DATA TYPE FLOAT USING latitude::double precision

ALTER TABLE dim_store_details
ALTER COLUMN country_code SET DATA TYPE VARCHAR(451)

ALTER TABLE dim_store_details
ALTER COLUMN continent SET DATA TYPE VARCHAR(255)

ALTER TABLE dim_store_details
ALTER COLUMN staff_numbers SET DATA TYPE SMALLINT USING staff_numbers::smallint

#Task 4

SELECT REPLACE(product_price,'£','') AS product_price  FROM dim_products

UPDATE dim_products
SET product_price  = REPLACE(product_price, '£', '')

ALTER TABLE dim_products
ADD COLUMN weight_class VARCHAR(1853)

#Task 5

ALTER TABLE dim_products
RENAME removed to still_available

ALTER TABLE dim_products
ALTER COLUMN product_price SET DATA TYPE FLOAT USING product_price::double precision

ALTER TABLE dim_products
ALTER COLUMN weight SET DATA TYPE FLOAT USING weight::double precision

ALTER TABLE dim_products
ALTER COLUMN EAN SET DATA TYPE VARCHAR(1853)

ALTER TABLE dim_products
ALTER COLUMN product_code SET DATA TYPE VARCHAR(1853)

ALTER TABLE dim_products
ALTER COLUMN date_added SET DATA TYPE DATE USING date_added::date

ALTER TABLE dim_products
ALTER COLUMN uuid SET DATA TYPE uuid USING uuid::uuid

ALTER TABLE dim_products
ALTER COLUMN still_available SET DATA TYPE BOOL USING still_available::boolean

ALTER TABLE dim_products
ALTER COLUMN weight_class SET DATA TYPE VARCHAR(1853)


#Task 6
ALTER TABLE dim_date_times
ALTER COLUMN month SET DATA TYPE VARCHAR(120122)

ALTER TABLE dim_date_times
ALTER COLUMN day SET DATA TYPE VARCHAR(120122)

ALTER TABLE dim_date_times
ALTER COLUMN year SET DATA TYPE VARCHAR(120122)

ALTER TABLE dim_date_times
ALTER COLUMN time_period SET DATA TYPE VARCHAR(120122)

ALTER TABLE dim_date_times
ALTER COLUMN date_uuid SET DATA TYPE uuid USING date_uuid::uuid

#Task 7

ALTER TABLE dim_card_details
ALTER COLUMN card_number SET DATA TYPE VARCHAR(15308)

ALTER TABLE dim_card_details
ALTER COLUMN expiry_date SET DATA TYPE VARCHAR(15308)

ALTER TABLE dim_card_details
ALTER COLUMN date_payment_confirmed SET DATA TYPE DATE

#Task 8
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

#Task9 

ALTER TABLE orders_table
ADD FOREIGN KEY (product_code) REFERENCES dim_products(product_code);
