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
    
    def list_db_tables(self, engine):
        inspector = inspect(engine)
        return inspector.get_table_names()
   
  
    
    def upload_to_db(self, my_bucket, dim_products
                     ):
        my_bucket.to_sql(dim_products, self.upload_engine, if_exists='replace', index=False)
        #df3.to_sql(dim_store_details, self.upload_engine, if_exists='replace', index=False)
        #my_bucket_2.to_sql(dim_date_times, self.upload_engine, if_exists='replace', index=False)
        #order_table.to_sql(orders_table, self.upload_engine, if_exists='replace', index=False)
       # df.to_sql(table_name, self.upload_engine, if_exists='replace', index=False)
     # df2.to_sql(dim_card_details, self.upload_engine, if_exists='replace', index=False)
   
    

