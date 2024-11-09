from datetime import datetime
import requests
import json
import pandas as pd
from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

# Define the API URL
network_info_url = "http://api.citybik.es/v2/networks" # network data
network_ids = []
bike_station_data_dfs = []
current_date = datetime.now()
date_str = current_date.strftime("%Y%m%d")


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


@task(log_prints=True)
def write_local(df: pd.DataFrame, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = f"citybike_data/{dataset_file}"
    df.to_parquet(path)
    return path


@task(log_prints=True)
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcp_cloud_storage_bucket_block = GcsBucket.load("gcs-citybike")
    gcp_cloud_storage_bucket_block.upload_from_path(from_path=path, to_path=path)
    return


@flow()
def etl_web_to_gcs() -> None:
    """The main ETL function"""
    df_networks = get_networks(network_info_url)

    network_dataset_file = Path(f"citybike_networks_{date_str}.parquet")
    path = write_local(df_networks, network_dataset_file)
    write_gcs(path)


    for network_id in network_ids:
        print(f"Processing network_id: {network_id}.")
        bike_station_data_dfs.append(get_bike_data(network_info_url, network_id))

    bike_station_data_df_final = pd.concat(bike_station_data_dfs, ignore_index=True)

    bike_station_dataset_file = Path(f"citybike_bike_stations_{date_str}.parquet")
    path = write_local(bike_station_data_df_final, bike_station_dataset_file)
    write_gcs(path)

    # Display the DataFrame
    # print(df_networks[(df_networks.city == 'New York, NY') | (df_networks.city == 'London')].head())
    # print(df_networks[(df_networks.network_id == 'abu-dhabi-careem-bike') | (df_networks.city == 'London')].head())
    # print(bike_station_data_df_final[df_bike_stations.network_id == 'citi-bike-nyc'].head())
    print(bike_station_data_df_final.head())
    # print(bike_station_data_df_final['network_id'].unique())


if __name__ == "__main__":
    etl_web_to_gcs()