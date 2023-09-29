import pandas as pd
import tabula
from tabula import read_pdf 
from flask import Flask
from flask_restful import Resource, Api, reqparse
import requests
import boto3
import json



class DataExtractor():
    
    def extract_data(self, engine, table_name):
        query = f"SELECT * FROM {table_name}"
        extract_df = pd.read_sql(query, engine)
        return extract_df
        
    def read_rds_table(self, database_connector, table_name):
        engine = database_connector.init_db_engine('db_creds.yml')
        read_df = self.extract_data(engine, table_name)
        return read_df

    
    def retrieve_pdf_data(self, database_connector, pdf_path):
        retrieve_df = tabula.read_pdf(pdf_path, pages='all')
        retrieve_df = pd.concat(tabula.read_pdf(pdf_path, pages='all'), ignore_index=True)
        print(len(retrieve_df))
        return retrieve_df
   
    def list_number_of_stores(self, num_stores_endpoint, header):
        response = requests.get(num_stores_endpoint, headers=header)
        data = response.json()
        number_of_stores = data['number_stores']
        return number_of_stores
    
    def retrieves_store_data(self, num_stores_endpoint, store_endpoint, header):
        number_of_stores = self.list_number_of_stores(num_stores_endpoint, header)
        for store_number in range(number_of_stores):
            if store_number == 0:
                response = requests.get(f'{store_endpoint}/{store_number}', headers=header)
                data = response.json()
                columns = list(data.keys())
                store_data = pd.DataFrame(data, columns=columns, index=[0])
                store_data.set_index("index", inplace=True)
                store_data = []
                store_data.append(store_data)
            else:
                response = requests.get(f'{store_endpoint}/{store_number}', headers=header)
                data = response.json()
                store_data = pd.DataFrame(data, index=[0])
                store_data.set_index("index", inplace=True)
                store_data.append(store_data)
        store_data = pd.concat(store_data, ignore_index=True)
        return store_data
    
    def extract_from_s3(self, s3_address):
        bucket, key = s3_address.replace('s3://', '').split('/', 1)
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=bucket, Key=key)
        body = response['Body']
        my_df_2 = pd.read_csv(body)
        
        return my_df_2
        
    def extract_from_s3_2(self, s3_address_2):
        bucket, key = s3_address_2.replace('s3://', '').split('/', 1)
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=bucket, Key=key)
        body = response['Body']
        my_df = pd.read_json(body)
        return my_df
       







