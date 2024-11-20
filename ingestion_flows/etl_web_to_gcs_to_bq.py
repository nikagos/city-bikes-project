import requests
import json
import pandas as pd
from datetime import datetime
from prefect import flow, task
from pathlib import Path
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from google.cloud import storage
from concurrent.futures import ThreadPoolExecutor


NETWORK_INFO_URL = "http://api.citybik.es/v2/networks"
NETWORK_IDS = []
DATASET_DFS = []
DATASET_FILES = []
BIKE_STATION_DATA_DFS = []
CURRENT_DATE = datetime.now().strftime("%Y%m%d")
PROJECT_NAME="steady-webbing-436216-k1"
COUNTRY_CODES = []


def get_networks(url: str) -> pd.DataFrame:
    """Fetch network data and store into a DataFrame"""
    print("Getting the list of networks...")

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

            for network in networks:
                COUNTRY_CODES.append(network["location"]["country"])
                if network["location"]["country"] in ("US"): #,"GR","GB","FR","JP","ES"):
                    parsed_data.append({
                        "network_id": network["id"],
                        "network_name": network["name"],
                        "company": network["company"][0] if network["company"] else None,
                        "city": network["location"]["city"],
                        "country": network["location"]["country"],
                        "latitude": network["location"]["latitude"],
                        "longitude": network["location"]["longitude"]
                    })

                    NETWORK_IDS.append(network["id"])

                else:
                    continue
                
            # Load data into a DataFrame
            df = pd.DataFrame(parsed_data)

            return df
            
        except json.JSONDecodeError:
            print("Error: Response content is not in JSON format.")
            print("Response content:", network_response.text)

    else:
        print(f"Failed to retrieve data: Status code {network_response.status_code}")
        print("Response content:", network_response.text)

        return None


# @task(log_prints=True)
def get_bike_data(network_id: str) -> pd.DataFrame:
    """Fetch bike station data and store into a DataFrame"""

    bike_station_url = f"{NETWORK_INFO_URL}/{network_id}" # bike station data

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
    directory = "citybike_data"
    Path(directory).mkdir(parents=True, exist_ok=True)  # Create directory if it doesn't exist
    path = f"{directory}/{dataset_file.stem}.parquet"
    df.to_parquet(path)
    return path


@task(log_prints=True)
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    print("Moving data from local machine into GCS bucket...")
    gcp_cloud_storage_bucket_block = GcsBucket.load("gcs-citybike")
    gcp_cloud_storage_bucket_block.upload_from_path(from_path=path, to_path=path)
    return


@task(log_prints=True)
def write_bq(df_name: str) -> None:
    """Write DataFrame to BiqQuery"""
    print("Moving data from GCS to BQ...")

    # GCP credentials
    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")
    gcp_credentials=gcp_credentials_block.get_credentials_from_service_account()
    
    # Initialize the Storage client with credentials
    storage_client = storage.Client(credentials=gcp_credentials)
    gcs_bucket_name = "de-zoomcamp-project-citybike-bucket"
    gcs_folder_path = "citybike_data"
    bucket = storage_client.bucket(gcs_bucket_name)

    blobs = bucket.list_blobs(prefix=gcs_folder_path)

    
    for blob in blobs:
        file_name = Path(blob.name.split("/")[-1])
        if file_name.suffix == ".parquet":
            table_name = file_name.stem #file_name.split("_")[0]
            
            # Read Parquet data into a DataFrame
            blob_uri = f"gs://{gcs_bucket_name}/{blob.name}"
            print(blob_uri)
            df = pd.read_parquet(blob_uri, storage_options={"token": gcp_credentials})


            destination_table=f"dezoomcamp.{table_name}"
            print(f"Destination table: {destination_table}")

            # Load into BigQuery using pandas-gbq
            df.to_gbq(
                destination_table=destination_table,
                project_id=PROJECT_NAME,
                credentials=gcp_credentials,
                chunksize=500_000,
                if_exists="replace"
            )

        print(f"Loaded {file_name} into BigQuery table {table_name}")



@flow()
def etl_web_to_gcs_to_bq() -> None:
    """The main ETL function"""

    # Ingest network data
    df_networks = get_networks(NETWORK_INFO_URL)
    print("Done with getting the list of networks.")

    df_networks.name = "networks"
    DATASET_DFS.append(df_networks)
    networks_file = Path(f"{df_networks.name}_{CURRENT_DATE}.parquet")
    DATASET_FILES.append(networks_file)

    # # Use this for testing a subset of network_id's
    # NETWORK_IDS_subset = NETWORK_IDS[10:30] 

    # Implement parallel processing due to the volume of data
    with ThreadPoolExecutor(max_workers=100) as executor: # Adjust max_workers as needed
        print("Getting the bike station data...")
        results = list(executor.map(get_bike_data, NETWORK_IDS)) # Substitute NETWORK_IDS with NETWORK_IDS_subset for testing
    
    # Stitch all station level data into a master dataframe
    bike_station_data_df_final = pd.concat(results, ignore_index=True)
    print("Done with getting the bike station data.")
    print(bike_station_data_df_final.groupby(['network_id']).size())

    bike_station_data_df_final.name = "bike_station_data"
    DATASET_DFS.append(bike_station_data_df_final)
    bike_station_data_file = Path(f"{bike_station_data_df_final.name}_{CURRENT_DATE}.parquet")
    DATASET_FILES.append(bike_station_data_file)


    for df, dataset_file in zip(DATASET_DFS, DATASET_FILES):
        
        # Save downloaded data locally
        path = write_local(df, dataset_file)
        
        # Write data into "citybike_data" GCS bucket
        write_gcs(path)

        # Write data into BigQuery tables
        write_bq(df.name)


if __name__ == "__main__":
    etl_web_to_gcs_to_bq()