import pandas as pd
import tabula
from tabula import read_pdf 

pdf_path = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"

class DataExtractor:
    
    def extract_data(self, engine, table_name):
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, engine)
        return df
    def read_rds_table(self, database_connector, table_name):
        engine = database_connector.init_db_engine('db_creds.yml')
        df = self.extract_data(engine, table_name)
        return df
    def retrieve_pdf_data(self, database_connector, pdf_path):
        # df2 = tabula.read_pdf(pdf_path, stream=True)
        df2 = tabula.read_pdf(pdf_path, pages='all')
        df2 = pd.concat(tabula.read_pdf(pdf_path, pages='all'), ignore_index=True)
        # read_pdf returns list of DataFrames
        print(len(df2))
        df2[0]
        return df2
        
    








