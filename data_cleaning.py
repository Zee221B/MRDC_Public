import pandas as pd
import numpy as np

class DataCleaning:
     @staticmethod
     def clean_user_data(user_data):
        user_data['first_name'] = user_data['first_name'].astype('string')
        user_data['last_name'] = user_data['last_name'].astype('string')
        user_data["date_of_birth"] = pd.to_datetime(user_data["date_of_birth"], infer_datetime_format=True, errors='coerce') 
        user_data["join_date"] = pd.to_datetime(user_data["join_date"], infer_datetime_format=True, errors='coerce') 
        user_data['company'] = user_data['company'].astype('category')
        user_data['email_address'] = user_data['email_address'].astype('string') 
        user_data['address'] = user_data['address'].astype('string')
        user_data['country'] = user_data['company'].astype('category')
        user_data['country_code'] = user_data['country_code'].astype('category')
        user_data['phone_number'] = user_data['phone_number'].astype('string')
        user_data['user_uuid'] = user_data['user_uuid'].astype('string')
        my_country_code = ["GB", "DE", "US"]
        user_data[user_data.country_code.isin(my_country_code)].set_index('country_code')
        my_countries = ["United Kingdom", "United States", "Germany"]
        user_data[user_data.country.isin(my_countries)].set_index('country')
        user_data['phone_number'].replace('.', '', inplace=True)
        user_data = user_data.reset_index(drop=True)
        user_data.dropna(inplace=True)
        return user_data
     
     def clean_card_data(self, card_data): 
         pdf_path = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
         unique_values = card_data['card_provider'].unique().tolist()
         values_to_remove = unique_values[-14:]
         card_data.drop(card_data[card_data['card_provider'].isin(values_to_remove)].index, inplace=True)
         card_data.duplicated().sum()
         card_data.duplicated(keep='first')
         my_card_provider = ["VISA 16 digit", "JCB 16 digit", "VISA 13 digit", "VISA 19 digit", "JCB 15 digit", "Diners Club / Carte Blanche", "American Express", "Maestro", "Discover", "Mastercard"]
         card_data[card_data.card_provider.isin(my_card_provider)].set_index('card_provider')
         card_data.replace('NULL', np.nan, inplace=True)
         card_data.dropna(inplace=True)
         card_data.drop(card_data[card_data['date_payment_confirmed'] == 'December 2021 17'].index, inplace=True)
         card_data.drop(card_data[card_data['date_payment_confirmed'] == 'December 2000 01'].index, inplace=True)
         card_data.drop(card_data[card_data['date_payment_confirmed'] == '2008 May 11'].index, inplace=True)
         card_data.drop(card_data[card_data['date_payment_confirmed'] == 'May 1998 09'].index, inplace=True)
         card_data.drop(card_data[card_data['date_payment_confirmed'] == '2005 July 01'].index, inplace=True)
         card_data.drop(card_data[card_data['date_payment_confirmed'] == 'September 2016 04'].index, inplace=True)
         card_data.drop(card_data[card_data['date_payment_confirmed'] == 'October 2000 04'].index, inplace=True)
         card_data.drop(card_data[card_data['date_payment_confirmed'] == '2017/05/15'].index, inplace=True)
         card_data["expiry_date"] = pd.to_datetime(card_data["expiry_date"], format='%m/%y') 
         card_data["date_payment_confirmed"] = pd.to_datetime(card_data["date_payment_confirmed"])
         card_data['card_provider'] = card_data['card_provider'].astype('category')
         card_data['card_number'] = card_data['card_number'].astype('string')
         card_data = card_data.reset_index(drop=True)
         return card_data 

     def clean_store_data(self, store_data):
         store_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details"
         num_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
         header = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
         store_data['address'] = store_data['address'].astype('string')
         store_data['longitude'] = store_data['longitude'].astype('string')
         store_data['latitude'] = store_data['latitude'].astype('string')
         store_data['lat'] = store_data['lat'].astype('string')
         store_data['locality'] = store_data['locality'].astype('string')
         store_data['store_code'] = store_data['store_code'].astype('string')
         store_data['staff_numbers'] = store_data['staff_numbers'].astype('string')
         store_data["opening_date"] = pd.to_datetime(store_data["opening_date"], infer_datetime_format=True, errors='coerce') 
         store_data['store_type'] = store_data['store_type'].astype('category')
         store_data['country_code'] = store_data['country_code'].astype('category')
         store_data['continent'] = store_data['continent'].astype('category')
         store_data.info()
         store_data.duplicated().sum()
         store_data.duplicated(keep='first')
         the_country_code = ["GB", "DE", "US"]
         store_data[store_data.country_code.isin(the_country_code)].set_index('country_code')
         my_continent = ["Europe"]
         store_data[store_data.country_code.isin(my_continent)].set_index('continent')
         my_store_code = ["Super Store", "Web Portal", "Local"]
         store_data[store_data.store_code.isin(my_store_code)].set_index('store_code')
         store_data.drop('lat', axis=1, inplace=True)
         store_data.dropna(inplace=True)
         store_data = store_data.reset_index(drop=True)
         return store_data 
     
     def convert_product_weights(self, my_bucket):
         my_bucket.dropna(inplace=True)
         my_bucket = my_bucket.reset_index(drop=True)
         my_bucket.loc[my_bucket['weight'] == 'ml', 'weight'] = my_bucket.loc[my_bucket['weight'] == 'ml', 'weight'] * 0.001
         my_bucket.loc[my_bucket['weight'] == 'g', 'weight'] = my_bucket.loc[my_bucket['weight'] == 'g', 'weight'] * 0.001
         my_bucket['weight'] = my_bucket['weight'].apply(lambda a : a.replace("kg", " ").replace("g", " ").replace("ml", " ").replace("oz", " "). replace("x", "").replace("  ", "") ) 
         return my_bucket
     
     def clean_products_data(self, my_bucket):
         my_bucket['product_name'] = my_bucket['product_name'].astype('string')
         my_bucket['product_price'] = my_bucket['product_price'].astype('float64')
         my_bucket["date_added"] = pd.to_datetime(my_bucket["date_added"], infer_datetime_format=True, errors='coerce') 
         my_bucket['category'] = my_bucket['category'].astype('category')
         my_bucket['weight'] = my_bucket['weight'].astype('float64') 
         my_bucket['EAN'] = my_bucket['EAN'].astype('string')
         my_bucket['removed'] = my_bucket['removed'].astype('category')
         my_bucket['product_code'] = my_bucket['product_code'].astype('string')
         my_bucket['uuid'] = my_bucket['uuid'].astype('string')
         my_bucket.duplicated().sum()
         my_bucket.duplicated(keep='first')
         my_bucket['product_price'] = my_bucket['product_price'].apply(lambda a : a.replace("£", " "))
         my_bucket = my_bucket.drop([751])
         my_bucket = my_bucket.drop([788])
         my_bucket = my_bucket.drop([794])
         my_bucket = my_bucket.drop([1133])
         my_bucket = my_bucket.drop([1401])
         my_bucket = my_bucket.drop([1400])
         my_bucket = my_bucket.drop([266])
         return my_bucket


     def clean_orders_data(self, order_table):
         order_table = order_table.drop('first_name', axis=1)
         order_table = order_table.drop('last_name', axis=1)
         order_table = order_table.drop('1', axis=1)
         return order_table
    
     def clean_date_events_data_2(self, my_bucket_2):
        my_bucket_2.duplicated().sum()
        my_bucket_2.duplicated(keep='first')
        my_bucket_2.dropna(inplace=True)
        my_bucket_2 = my_bucket_2.reset_index(drop=True)
        my_bucket_2['date_uuid'] = my_bucket_2['date_uuid'].astype('string')
        my_bucket_2['time_period'] = my_bucket_2['time_period'].astype('category')
        my_bucket_2["day"] = pd.to_datetime(my_bucket_2["day"],  errors='coerce')
        my_bucket_2["year"] = pd.to_datetime(my_bucket_2["year"], errors='coerce')
        my_bucket_2["month"] = pd.to_datetime(my_bucket_2["month"],  errors='coerce')
        my_bucket_2["timestamp"] = pd.to_datetime(my_bucket_2["timestamp"], errors='coerce')
        my_time_period =  ["Morning", "Evening", "Midday", "Late_Hours"]
        my_bucket_2[my_bucket_2.time_period.isin(my_time_period)].set_index('time_period')
        return my_bucket_2
   



         
