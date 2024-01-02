import os
import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine
from sqlalchemy import create_engine, text

def combine_npi_files(folder_path):
    postgres_engine = create_engine('postgresql://postgres:Aadil123@localhost:5432/aadil')
    csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
    combined_df = pd.DataFrame()

    for file in csv_files:
        file_path = os.path.join(folder_path, file)
        df = pd.read_csv(file_path)
        # combined_df = combined_df.append(df, ignore_index=True)
        combined_df = pd.concat([combined_df, df], ignore_index=True)

    combined_df.columns = combined_df.columns.str.strip()

    cols_to_pick = ['Provider Last Name (Legal Name)', 'Provider First Name', 'Provider Middle Name', 'NPI']
    selected_df = combined_df[cols_to_pick]
    selected_df.reset_index(drop=True, inplace=True)

    selected_df = selected_df.rename(columns={
        'Provider Last Name (Legal Name)': 'last_name',
        'Provider First Name': 'first_name',
        'Provider Middle Name': 'middle_name',
        'NPI': 'npi_number'
    })

    postgres_connection = postgres_engine.connect()
    selected_df.to_sql('individual', postgres_connection, schema='ish_qa', index=False, if_exists='append', method='multi')

    return selected_df

def combine_ep_files(folder_path):
    postgres_engine = create_engine('postgresql://postgres:Aadil123@localhost:5432/aadil')
    csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
    combined_df = pd.DataFrame()

    for file in csv_files:
        file_path = os.path.join(folder_path, file)
        df = pd.read_csv(file_path)
        # combined_df = combined_df.append(df, ignore_index=True)
        combined_df = pd.concat([combined_df, df], ignore_index=True)
        # combined_df = pd.concat([combined_df, df], ignore_index=True)

    combined_df.columns = combined_df.columns.str.strip()

    cols_to_pick = ['Affiliation Address Line One', 'Affiliation Address Line Two', 'Affiliation Address City', 'Affiliation Address State', 'Affiliation Address Postal Code']
    selected_df = combined_df[cols_to_pick]

    selected_df['Affiliation Address Postal Code'] = selected_df['Affiliation Address Postal Code'].astype(str)

    selected_df['zip'] = selected_df['Affiliation Address Postal Code'].str.slice(0, 4)
    selected_df['zip4'] = selected_df['Affiliation Address Postal Code'].str.slice(4)

    selected_df.reset_index(drop=True, inplace=True)

    rename_dict = {
        'Affiliation Address Line One': 'address1',
        'Affiliation Address Line Two': 'address2',
        'Affiliation Address City' : 'city',
        'Affiliation Address State' : 'state'
        # 'Affiliation Address Postal Code' : 'Postal Code'
    }

    selected_df = selected_df.drop('Affiliation Address Postal Code', axis=1)

    # Convert 'zip' and 'zip4' to integers, fill NaN values with 0
    selected_df['zip'] = pd.to_numeric(selected_df['zip'], errors='coerce').fillna(0).astype(int)
    selected_df['zip4'] = pd.to_numeric(selected_df['zip4'], errors='coerce').fillna(0).astype(int)

    selected_df.rename(columns=lambda x: rename_dict.get(x.strip(), x), inplace=True)

    postgres_connection = postgres_engine.connect()
    try:
        selected_df.to_sql('location', postgres_connection, schema='ish_qa', index=False, if_exists='append', method='multi',chunksize=1000)
    except Exception as e:
        print("-----------------------------")
        print("Error during insertion:", e)
        print("Problematic data:")
        
    print(selected_df.dtypes)

    return selected_df

def read_location_and_individual_ids():
    postgres_engine = create_engine('postgresql://postgres:Aadil123@localhost:5432/aadil')

    # Read individual_id from the 'individual' table
    query_individual = "SELECT individual_id FROM ish_qa.individual i"
    individual_df = pd.read_sql_query(query_individual, postgres_engine)


    query_location = "SELECT location_id FROM ish_qa.location l"
    location_df = pd.read_sql_query(query_location, postgres_engine)

    # Combine individual_id and location_id into a single DataFrame
    result_df = pd.concat([individual_df, location_df], axis=1)

    result_df['is_active'] = 1

    rename_dict = {
        'individual_df': 'individual_id',
        'location_df': 'location_id'
    }

    # result_df.rename(columns=lambda x: rename_dict.get(x.strip(), x), inplace=True)
    result_df.rename(columns=rename_dict, inplace=True)

    postgres_connection = postgres_engine.connect()
    try:
        result_df.to_sql('individual_location_association', postgres_connection, schema='ish_qa', index=False, if_exists='append', method='multi',chunksize=1000)
    except Exception as e:
        print("-----------------------------")
        print("Error:", e)
    postgres_connection.commit()
    postgres_connection.close()
    return result_df

if __name__ == "__main__":
    postgres_engine = create_engine('postgresql://postgres:Aadil123@localhost:5432/aadil')

    folder_path1 = 'C:/Users/Er. Aadil Ji/Desktop/sagar/npi_data'
    combined_data1 = combine_npi_files(folder_path1)

    folder_path2 = 'C:/Users/Er. Aadil Ji/Desktop/sagar/endpoint_data'
    combined_data2 = combine_ep_files(folder_path2)

    read_location_and_individual_ids()

    combined_data1.to_csv('1.csv')

    combined_data2.to_csv('2.csv')
