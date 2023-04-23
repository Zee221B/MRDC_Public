import pandas as pd
import tabula
from tabula import read_pdf 
from flask import Flask
from flask_restful import Resource, Api, reqparse
import requests
import boto3
import json



class DataExtractor(Resource):
    
    def extract_data(self, engine, table_name):
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, engine)
        return df
        
    def read_rds_table(self, database_connector, table_name):
        engine = database_connector.init_db_engine('db_creds.yml')
        df = self.extract_data(engine, table_name)
        return df

    
    def retrieve_pdf_data(self, database_connector, pdf_path):
        df2 = tabula.read_pdf(pdf_path, pages='all')
        df2 = pd.concat(tabula.read_pdf(pdf_path, pages='all'), ignore_index=True)
        print(len(df2))
        return df2
   
    def list_number_of_stores(self, num_stores_endpoint, header):
        response = requests.get(num_stores_endpoint, headers=header)
        #print(response)
        data = response.json()
        #print(data)
        number_of_stores = data['number_stores']
        return number_of_stores
    
    def retrieves_store_data(self, num_stores_endpoint, store_endpoint, header):
        number_of_stores = self.list_number_of_stores(num_stores_endpoint, header)
        for store_number in range(number_of_stores):
            if store_number == 0:
                response = requests.get(f'{store_endpoint}/{store_number}', headers=header)
                data = response.json()
                #print(data)
                columns = list(data.keys())
                #print(columns)
                df3 = pd.DataFrame(data, columns=columns, index=[0])
                df3.set_index("index", inplace=True)
                #print(df3)
                dfs = []
                dfs.append(df3)
            else:
                response = requests.get(f'{store_endpoint}/{store_number}', headers=header)
                data = response.json()
                df3 = pd.DataFrame(data, index=[0])
                df3.set_index("index", inplace=True)
                dfs.append(df3)
        df3 = pd.concat(dfs, ignore_index=True)
        return df3
    
    def extract_from_s3(self, s3_address):
        """Extracts data from an S3 bucket and returns a pandas DataFrame."""
        
        # Split the S3 address into its components
        bucket, key = s3_address.replace('s3://', '').split('/', 1)
        
        # Create a client for accessing the S3 bucket and 
        # informing boto3 that we intend to use an S3 bucket

        s3 = boto3.client('s3')
        
        # Download the file from S3
        response = s3.get_object(Bucket=bucket, Key=key)
        body = response['Body']
        
        # Read the contents of the file into a pandas DataFrame
        my_bucket = pd.read_csv(body)
        
        return my_bucket
        
    def extract_from_s3_2(self, s3_address_2):
        """Extracts data from an S3 bucket and returns a pandas DataFrame."""
        
        # Split the S3 address into its components
        bucket, key = s3_address_2.replace('s3://', '').split('/', 1)
        
        # Create a client for accessing the S3 bucket and 
        # informing boto3 that we intend to use an S3 bucket

        s3 = boto3.client('s3')
        
        # Download the file from S3
        response = s3.get_object(Bucket=bucket, Key=key)
        body = response['Body']
        
        # Read the contents of the file into a pandas DataFrame
        my_bucket_2 = pd.read_json(body)
        
        return my_bucket_2
       







