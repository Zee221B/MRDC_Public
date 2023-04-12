import yaml
import sqlalchemy 
from sqlalchemy import create_engine
from sqlalchemy import inspect
from data_extraction import DataExtractor
from data_cleaning import DataCleaning



class DatabaseConnector:
    def __init__(self):
        self.engine = self.init_db_engine('db_creds.yml')
        self.upload_engine = self.init_db_engine('db_creds2.yml')

    def init_db_engine(self, creds_file):
        creds = self.read_db_creds(creds_file)
        engine = create_engine(f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}")
        return engine

    def read_db_creds(self, creds_file):
        with open(creds_file, 'r') as file:
            creds = yaml.safe_load(file)
        return creds
    
    def list_db_tables(self):
        inspector = inspect(engine)
        return inspector.get_table_names()
    
    def upload_to_db(self, df, df2, df3, dim_users, dim_card_details, dim_store_details):
        df.to_sql(dim_users, self.upload_engine, if_exists='replace', index=False)
        df2.to_sql(dim_card_details, self.upload_engine, if_exists='replace', index=False)
        df3.to_sql(dim_store_details, self.upload_engine, if_exists='replace', index=False)
    
    
database_connector = DatabaseConnector()
data_extractor = DataExtractor()
data_cleaning = DataCleaning()


# engine = database_connector.init_db_engine('db_creds.yml')
# upload_engine = database_connector.init_db_engine('db_creds2.yml')
# table_names = database_connector.list_db_tables()
# print(table_names)

# pdf_path = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf" 

# store_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details"
# num_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
# header = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
# #stores_df = data_extractor.retrieves_store_data(num_stores_endpoint, store_endpoint, header)


# user_df = data_extractor.read_rds_table(database_connector, table_names[1])
# df2 = data_extractor.retrieve_pdf_data(database_connector, pdf_path)
# df3 = data_extractor.retrieves_store_data(num_stores_endpoint, store_endpoint, header)


# cleaned_df = data_cleaning.clean_user_data(user_df)
# cleaned_df2 = data_cleaning.clean_card_data(df2)
# cleaned_df3 = data_cleaning.clean_store_data(df3)
# database_connector.upload_to_db(cleaned_df, cleaned_df2, cleaned_df3, "dim_users", "dim_card_details", "dim_store_details")

# print(cleaned_df)
# print(cleaned_df2)
# print(cleaned_df3)






