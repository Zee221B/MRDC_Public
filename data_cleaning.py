import pandas as pd
import numpy as np

class DataCleaning:
     @staticmethod
     def clean_user_data(user_data):
        
       
        # Convert each object to correct datatypes
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
       
        

        #Identify and remove null/gibberish values
       
        
        my_country_code = ["GB", "DE", "US"]
        user_data[user_data.country_code.isin(my_country_code)].set_index('country_code')
        my_countries = ["United Kingdom", "United States", "Germany"]
        user_data[user_data.country.isin(my_countries)].set_index('country')

        #Eliminate unwanted characters from phone numbers
        user_data['phone_number'].replace('.', '', inplace=True)

        # Reset index
        user_data = user_data.reset_index(drop=True)
        user_data.dropna(inplace=True)

        
        return user_data
     
     def clean_card_data(self, df2):
         
         pdf_path = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
       
         # Convert each object to correct datatypes
         unique_values = df2['card_provider'].unique().tolist()
         values_to_remove = unique_values[-14:]
         df2.drop(df2[df2['card_provider'].isin(values_to_remove)].index, inplace=True)
         df2.duplicated().sum()
         df2.duplicated(keep='first')
         my_card_provider = ["VISA 16 digit", "JCB 16 digit", "VISA 13 digit", "VISA 19 digit", "JCB 15 digit", "Diners Club / Carte Blanche", "American Express", "Maestro", "Discover", "Mastercard"]
         df2[df2.card_provider.isin(my_card_provider)].set_index('card_provider')
         df2.replace('NULL', np.nan, inplace=True)
         df2.dropna(inplace=True)
         df2.drop(df2[df2['date_payment_confirmed'] == 'December 2021 17'].index, inplace=True)
         df2.drop(df2[df2['date_payment_confirmed'] == 'December 2000 01'].index, inplace=True)
         df2.drop(df2[df2['date_payment_confirmed'] == '2008 May 11'].index, inplace=True)
         df2.drop(df2[df2['date_payment_confirmed'] == 'May 1998 09'].index, inplace=True)
         df2.drop(df2[df2['date_payment_confirmed'] == '2005 July 01'].index, inplace=True)
         df2.drop(df2[df2['date_payment_confirmed'] == 'September 2016 04'].index, inplace=True)
         df2.drop(df2[df2['date_payment_confirmed'] == 'October 2000 04'].index, inplace=True)
         df2.drop(df2[df2['date_payment_confirmed'] == '2017/05/15'].index, inplace=True)


         df2["expiry_date"] = pd.to_datetime(df2["expiry_date"], format='%m/%y') 
         df2["date_payment_confirmed"] = pd.to_datetime(df2["date_payment_confirmed"])
         df2['card_provider'] = df2['card_provider'].astype('category')
         df2['card_number'] = df2['card_number'].astype('string')
         df2 = df2.reset_index(drop=True)
         
         return df2 
    
    
     def clean_store_data(self, df3):
         
         store_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details"
         num_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
         header = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}

         # Convert each object to correct datatypes
         df3['address'] = df3['address'].astype('string')
         df3['longitude'] = df3['longitude'].astype('string')
         df3['latitude'] = df3['latitude'].astype('string')
         df3['lat'] = df3['lat'].astype('string')
         df3['locality'] = df3['locality'].astype('string')
         df3['store_code'] = df3['store_code'].astype('string')
         df3['staff_numbers'] = df3['staff_numbers'].astype('string')
         df3["opening_date"] = pd.to_datetime(df3["opening_date"], infer_datetime_format=True, errors='coerce') 
         df3['store_type'] = df3['store_type'].astype('category')
         df3['country_code'] = df3['country_code'].astype('category')
         df3['continent'] = df3['continent'].astype('category')
         df3.info()

         # Identify any duplicate values
         df3.duplicated().sum()
         df3.duplicated(keep='first')
        
         #Identify and remove null/gibberish values
         the_country_code = ["GB", "DE", "US"]
         df3[df3.country_code.isin(the_country_code)].set_index('country_code')
         my_continent = ["Europe"]
         df3[df3.country_code.isin(my_continent)].set_index('continent')
         my_store_code = ["Super Store", "Web Portal", "Local"]
         df3[df3.store_code.isin(my_store_code)].set_index('store_code')
         df3.drop('lat', axis=1, inplace=True)
         df3.dropna(inplace=True)
         df3 = df3.reset_index(drop=True)

    
         return df3 
     
     def convert_product_weights(self, my_bucket):
         
         #Identify and remove null/gibberish values
         my_bucket.dropna(inplace=True)
         my_bucket = my_bucket.reset_index(drop=True)
       
     # Convert all weights to kg and represent as a float
         my_bucket.loc[my_bucket['weight'] == 'ml', 'weight'] = my_bucket.loc[my_bucket['weight'] == 'ml', 'weight'] * 0.001
         my_bucket.loc[my_bucket['weight'] == 'g', 'weight'] = my_bucket.loc[my_bucket['weight'] == 'g', 'weight'] * 0.001
       
          #removed all kg and g using  replace method and from every row using apply lambda
         my_bucket['weight'] = my_bucket['weight'].apply(lambda a : a.replace("kg", " ").replace("g", " ").replace("ml", " ").replace("oz", " "). replace("x", "").replace("  ", "") ) 
       
        # my_bucket['weight'] = my_bucket['weight'].astype('float64')

         return my_bucket
     
     def clean_products_data(self, my_bucket):
         #Convert each object to correct datatypes
         my_bucket['product_name'] = my_bucket['product_name'].astype('string')
         my_bucket['product_price'] = my_bucket['product_price'].astype('float64')
         my_bucket["date_added"] = pd.to_datetime(my_bucket["date_added"], infer_datetime_format=True, errors='coerce') 
         my_bucket['category'] = my_bucket['category'].astype('category')
        # my_bucket['weight'] = my_bucket['weight'].astype('float64') 
         my_bucket['EAN'] = my_bucket['EAN'].astype('string')
         my_bucket['removed'] = my_bucket['removed'].astype('category')
         my_bucket['product_code'] = my_bucket['product_code'].astype('string')
         my_bucket['uuid'] = my_bucket['uuid'].astype('string')

        #identify any dupilicate values
         my_bucket.duplicated().sum()
         my_bucket.duplicated(keep='first')

        #remove £ symbol from product_price column
         my_bucket['product_price'] = my_bucket['product_price'].apply(lambda a : a.replace("£", " "))
        
          #remove gibberish rows

        
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
        #Find duplicates 
        my_bucket_2.duplicated().sum()
        my_bucket_2.duplicated(keep='first')

        #Find null values 
        my_bucket_2.dropna(inplace=True)
        my_bucket_2 = my_bucket_2.reset_index(drop=True)
        
        # convert to correct datatypes - DONE

        my_bucket_2['date_uuid'] = my_bucket_2['date_uuid'].astype('string')
        my_bucket_2['time_period'] = my_bucket_2['time_period'].astype('category')
        my_bucket_2["day"] = pd.to_datetime(my_bucket_2["day"],  errors='coerce')
        my_bucket_2["year"] = pd.to_datetime(my_bucket_2["year"], errors='coerce')
        my_bucket_2["month"] = pd.to_datetime(my_bucket_2["month"],  errors='coerce')
        my_bucket_2["timestamp"] = pd.to_datetime(my_bucket_2["timestamp"], errors='coerce')

        #Remvove gibberish values
        my_time_period =  ["Morning", "Evening", "Midday", "Late_Hours"]
        my_bucket_2[my_bucket_2.time_period.isin(my_time_period)].set_index('time_period')
       
        return my_bucket_2
   



         
