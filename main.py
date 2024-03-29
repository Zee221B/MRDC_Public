from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from database_utils import DatabaseConnector

database_connector = DatabaseConnector()
data_extractor = DataExtractor()
data_cleaning = DataCleaning()

engine = database_connector.init_db_engine('db_creds.yml')
upload_engine = database_connector.init_db_engine('db_creds2.yml')
table_names = database_connector.list_db_tables(engine)
print(table_names)


pdf_path = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf" 
store_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details"
num_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
header = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
stores_df = data_extractor.retrieves_store_data(num_stores_endpoint, store_endpoint, header)
s3_address = "s3://data-handling-public/products.csv" 
s3_address_2 = "s3://myaicorebucket/date_details.json"

user_df = data_extractor.read_rds_table(database_connector, table_names[2])
df2 = data_extractor.retrieve_pdf_data(database_connector, pdf_path)
df3 = data_extractor.retrieves_store_data(num_stores_endpoint, store_endpoint, header)
my_bucket = data_extractor.extract_from_s3(s3_address)
my_bucket_2 = data_extractor.extract_from_s3_2(s3_address_2)
tables = database_connector.list_db_tables(engine)
order_table = data_extractor.read_rds_table(database_connector, table_names[2])


cleaned_df = data_cleaning.clean_user_data(user_df)
cleaned_df2 = data_cleaning.clean_card_data(df2)
cleaned_df3 = data_cleaning.clean_store_data(df3)
cleaned_my_bucket = data_cleaning.convert_product_weights(my_bucket)
cleaned_orders_table = data_cleaning.clean_orders_data(order_table)
cleaned_my_bucket_2 = data_cleaning.clean_date_events_data_2(my_bucket_2)

database_connector.upload_to_db(cleaned_df, "dim_users")
database_connector.upload_to_db(cleaned_df2, "dim_card_details")
database_connector.upload_to_db(cleaned_df3, "dim_store_details")
database_connector.upload_to_db(cleaned_my_bucket, "dim_products")
database_connector.upload_to_db(cleaned_orders_table, "orders_table")
database_connector.list_db_tables(engine)
database_connector.upload_to_db(cleaned_my_bucket_2, "dim_date_times")

num_rows = cleaned_df2.shape[0]
print(num_rows)