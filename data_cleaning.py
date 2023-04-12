import pandas as pd

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
        print("*" * 40)
        user_data.info()

        #Identify and remove null/gibberish values
        print("*" * 40)
        user_data.info()
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
         df2.info()
         df2['card_number'] = df2['card_number'].astype('string')
         df2["expiry_date"] = pd.to_datetime(df2["expiry_date"], infer_datetime_format=True, errors='coerce') 
         df2["date_payment_confirmed"] = pd.to_datetime(df2["date_payment_confirmed"], infer_datetime_format=True, errors='coerce')
         df2['card_provider'] = df2['card_provider'].astype('category')
        

         # Identify any duplicate values
         df2.duplicated().sum()
         df2.duplicated(keep='first')

         #Identify and remove null/gibberish values
         my_card_provider = ["VISA 16 digit", "JCB 16 digit", "VISA 13 digit", "VISA 19 digit", "JCB 15 digit", "Diners Club / Carte Blanche", "American Express", "Maestro", "Discover", "Mastercard"]
         df2[df2.card_provider.isin(my_card_provider)].set_index('card_provider')
         df2.dropna(inplace=True)
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
     
     #def convert_product_weights(self, df4):
         
