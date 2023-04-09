import pandas as pd

class DataCleaning:
     @staticmethod
     def clean_user_data(user_data):
        
       
        # Convert each object to correct datatypes
        user_data.info()

        user_data['first_name'] = user_data['first_name'].astype('string')
        user_data['first_name']

        user_data['last_name'] = user_data['last_name'].astype('string')
        user_data['last_name']

        user_data["date_of_birth"]
        user_data["date_of_birth"] = pd.to_datetime(user_data["date_of_birth"], infer_datetime_format=True, errors='coerce') 

        user_data["join_date"]
        user_data["join_date"] = pd.to_datetime(user_data["join_date"], infer_datetime_format=True, errors='coerce') 


        user_data['company'] = user_data['company'].astype('category')
        user_data['company']

        user_data['email_address'] = user_data['email_address'].astype('string')
        user_data['email_address']
 
        user_data['address'] = user_data['address'].astype('string')
        user_data['address']

        user_data['country'] = user_data['company'].astype('category')
        user_data['country']

        user_data['country_code'] = user_data['country_code'].astype('category')
        user_data['country_code']

        user_data['phone_number'] = user_data['phone_number'].astype('string')
        user_data['phone_number']

   
        user_data['user_uuid'] = user_data['user_uuid'].astype('string')
        user_data['user_uuid']

        # Identify any duplicate values
        user_data.duplicated().sum()

        #Identify and remove null/gibberish values
       
        dfresult = user_data['company'].dropna()
        print(dfresult)
        
        
        my_country_code = ["GB", "DE", "US"]
        user_data[user_data.country_code.isin(my_country_code)].set_index('country_code')

        my_countries = ["United Kingdom", "United States", "Germany"]
       

        user_data[user_data.country.isin(my_countries)].set_index('country')

       
        dfresult = user_data['date_of_birth'].dropna()
        print(dfresult)
     
        dfresult = user_data['join_date'].dropna()
        print(dfresult)

        #Eliminate unwanted characters from phone numbers
        user_data['phone_number'].replace('.', '', inplace=True)
        print(user_data['phone_number'])

        
        # Reset index
        cleaned_df = user_data.reset_index(drop=True)

        
        return cleaned_df
     
     def clean_card_data(df2):
         
         pdf_path = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
       
         # Convert each object to correct datatypes
         df2.info()

         df2['card_number'] = df2['card_number'].astype('string')
         df2['card_number']

         df2["expiry_date"]
         df2["expiry_date"] = pd.to_datetime(df2["expiry_date"], infer_datetime_format=True, errors='coerce') 

         df2["date_payment_confirmed"]
         df2["date_payment_confirmed"] = pd.to_datetime(df2["date_payment_confirmed"], infer_datetime_format=True, errors='coerce')

         df2['card_provider'] = df2['card_provider'].astype('category')
         df2['card_provider']

         # Identify any duplicate values

         df2.duplicated().sum()
         df2.duplicated(keep='first')

         #Identify and remove null/gibberish values
         df2['card_provider'].value_counts()
         dfresult = df2['card_provider'].dropna()
         print(dfresult)

         df2['card_provider'].value_counts()
         my_card_provider = ["VISA 16 digit", "JCB 16 digit", "VISA 13 digit", "VISA 19 digit", "JCB 15 digit", "Diners Club / Carte Blanche", "American Express", "Maestro", "Discover", "Mastercard"]
         print(my_card_provider)
         df2[df2.card_provider.isin(my_card_provider)].set_index('card_provider')

         df2['date_payment_confirmed'].value_counts()
         dfresult = df2['date_payment_confirmed'].dropna()
         print(dfresult)

         df2['card_number'].value_counts()
         dfresult = df2['card_number'].dropna()
         print(dfresult)

         df2['expiry_date'].value_counts()
         dfresult = df2['expiry_date'].dropna()
         print(dfresult)


         cleaned_df2 = pdf_path.reset_index(drop=True)
         
         return cleaned_df2 
         

