import argparse, os, sys
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from datetime import datetime
import requests
import json
import pandas as pd
from pathlib import Path
from prefect import flow, task

# Define the API URL
network_info_url = "http://api.citybik.es/v2/networks" # network data
network_ids = []
bike_station_data_dfs = []
current_date = datetime.now()
date_str = current_date.strftime("%Y%m%d")


@task(log_prints=True)
def create_postgres_engine(params) -> Engine:

    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    return engine


@task(log_prints=True)
def get_networks(url: str) -> pd.DataFrame:
    """Fetch network data and store into a DataFrame"""

    # Send a GET request to the API
    network_response = requests.get(url)

    # Check if the request was successful
    if network_response.status_code == 200:
        try:
            # Parse JSON data
            data = network_response.json()
            
            # Extract the list of networks
            networks = data['networks']
            
            # Flatten the data and select relevant fields
            parsed_data = []
            parsed_bike_station_data = []

            for network in networks:
                if network["location"]["country"] == "US":
                    parsed_data.append({
                        "network_id": network["id"],
                        "network_name": network["name"],
                        "company": network["company"][0] if network["company"] else None,
                        "city": network["location"]["city"],
                        "country": network["location"]["country"],
                        "latitude": network["location"]["latitude"],
                        "longitude": network["location"]["longitude"]
                    })

                    network_ids.append(network["id"])

                else:
                    continue
                
            # Load data into a DataFrame
            df = pd.DataFrame(parsed_data)
            
            # Display the DataFrame
            # print(df[df.city == 'New York, NY'].head())

            return df[df.country == "US"]
            
        except json.JSONDecodeError:
            print("Error: Response content is not in JSON format.")
            print("Response content:", network_response.text)

    else:
        print(f"Failed to retrieve data: Status code {network_response.status_code}")
        print("Response content:", network_response.text)

        return None


@task(log_prints=True)
def get_bike_data(url: str, network_id: str) -> pd.DataFrame:
    """Fetch bike station data and store into a DataFrame"""

    bike_station_url = f"{url}/{network_id}" # bike station data

    # Send a GET request to the API
    bike_station_response = requests.get(bike_station_url)

    # Check if the request was successful
    if bike_station_response.status_code == 200:
        try:
            # Parse JSON data
            data = bike_station_response.json()
            
            # Extract the list of networks
            stations = data['network']['stations']
            
            # Flatten the data and select relevant fields
            parsed_data = []
            for station in stations:
                parsed_data.append({
                    "network_id": data["network"]["id"],
                    "station_id": station["id"],
                    "station_name": station["name"],
                    "timestamp": station["timestamp"],
                    "latitude": station["latitude"],
                    "longitude": station["longitude"],
                    "free_bikes": station["free_bikes"],
                    "empty_slots": station["empty_slots"]
                })

            # Load data into a DataFrame
            df_bike_data = pd.DataFrame(parsed_data)
            
            # # Display the DataFrame
            # print(df_bike_data.head())

            return df_bike_data
            
        except json.JSONDecodeError:
            print("Error: Response content is not in JSON format.")
            print("Response content:", bike_station_response.text)
    else:
        print(f"Failed to retrieve data: Status code {bike_station_response.status_code}")
        print("Response content:", bike_station_response.text)

        return None


def ingest_into_postgres(df: pd.DataFrame, engine: Engine, table_name: str) -> None:

    print('Creating table in the database: %s' % table_name)
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    print("Table created.")
    df.to_sql(name=table_name, con=engine, if_exists='append')
    print("Data was ingested.")


@flow()
def etl_web_to_postgres(args) -> None:
    """The main ETL function"""

    engine = create_postgres_engine(args)

    # Ingest network data
    df_networks = get_networks(network_info_url)
    df_networks.name = "networks"
    ingest_into_postgres(df_networks, engine, df_networks.name)


    # Ingest bike station data
    for network_id in network_ids:
        print(f"Processing network_id: {network_id}.")
        bike_station_data_dfs.append(get_bike_data(network_info_url, network_id))

    bike_station_data_df_final = pd.concat(bike_station_data_dfs, ignore_index=True)
    bike_station_data_df_final.name = "bike_station_data"
    ingest_into_postgres(bike_station_data_df_final, engine, bike_station_data_df_final.name)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Ingest Parquet data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')

    args = parser.parse_args()

    etl_web_to_postgres(args)