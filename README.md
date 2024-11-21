# The City Bike data pipeline


## About the City Bike data
The [CityBike API](https://api.citybik.es/v2/) offers open data about bike sharing across the world. There are two endpoints that people can use: one with the networks and one for the bike and station data of each network. Because the data is huge and the API gets overwhelmed after multiple calls, we focus on the US-based networks and their data only.


## Purpose of the project
Demonstrate how to build a pipeline (web -> data warehouse) using Google Cloud services, specifically GC Storage and BigQuery. The visualization piece is skipped as the main focus is the data extraction, loading, and transformation.


## Pipeline architecture
The pipeline utilizes a wide range of tools that are nowadays part of the modern data engineering stack. More specifically:
1. **Terraform** as the Infrastructure as Code tool to spin up the compute and storage resources of GCP
2. **Docker** to containerize the Python ingestion processes
3. **Google Cloud Storage Buckets** for raw data storage
4. **BigQuery** as the data warehouse for analytics and data exploration
6. **Prefect** as the job orchestrator
7. **dbt** for the transformations

<img src="https://github.com/nikagos/city-bikes-project/blob/master/images/citybike_project_architecture.png" width="1000">

Note that I built a virtual machine in GCP because they provide 90-day/$300 credit to access their tools.


## Setting up the environment
All the tools in the diagram above need to be installed on your machine. This can be complex and time-consuming to outline in detail here, so I strongly encourage you to watch the following videos. For dbt specifically, I've described the detailed steps below, but there are plenty of resources on the dbt website and online to accomplish this
1. Python 3.9 (Anaconda distribution) [Video](https://www.youtube.com/watch?v=ae-CV2KfoN0&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=15)
2. Terraform: [Video 1](https://www.youtube.com/watch?v=s2bOYDCKl_M&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=13), [Video 2](https://www.youtube.com/watch?v=Y2ux7gq3Z0o&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=13), [Video 3](https://www.youtube.com/watch?v=PBi0hHjLftk&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=15)
3. Docker and Docker Compose [Video](https://www.youtube.com/watch?v=ae-CV2KfoN0&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=16)
4. Prefect [Video 1](https://www.youtube.com/watch?v=cdtN6dhp708&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb), [Video 2](https://www.youtube.com/watch?v=cdtN6dhp708&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)
6. dbt Core for BigQuery
  * Confirm that you have the right Python version installed (3.7 or higher)
```
python --version
```
or
```
python3 --version
``` 
  * Install `dbt-core`. Also, because we are using Postgres, we need to install the `dbt-postgres` connector too along with dbt-core. In your terminal run:
```
pip install dbt-bigquery
```
  * Install dbt dependencies:
```
dbt deps
```
  * Configure Your dbt Profile. dbt requires a `profiles.yml` file to connect to your database. This file is typically located in the `~/.dbt/` directory. Create or edit the `profiles.yml` file such as:
```
citybike_dbt_project:
  outputs:
    dev:
      dataset: your_dataset_name
      job_execution_timeout_seconds: 600
      job_retries: 1
      keyfile: /path/to/your-service-account-key.json
      location: US
      method: service-account
      priority: interactive
      project: your-gcp-project-id
      threads: 1
      type: bigquery
  target: dev
```
Notice that we have a **dev** . 

After completing these steps, your dbt environment is set up and you can use the assets of this repo to create your dbt models too. Test the environment by doing:
* `dbt debug` to test if dbt can connect to BigQuery using the settings in profiles.yml
* `dbt run` to build the default test models that come in every dbt project
* `dbt test` to run the default tests against these test models. One test should fail.


## Before you run the pipeline
1. GCP Credentials have been stored in a Prefect Block (Prefect > Blocks > Create > Select "GCP Credentials")
2. GCS Bucket information has been stored in a Prefect Block (Prefect > Blocks > Create > Select "GCS Bucket"). Note that this requires the GCP Credentials to be available
3. Prefect Server is running (run command `prefect server start`)
4. Port 4200 (Prefect) has been forwarded. I use VS Code for this and the Ports > Forward option
5. Confirm that in your browser you can access:
  * Prefect: http://localhost:4200/


## Run the pipeline
1. Open up your terminal and clone the repo locally
2. Navigate to `/city-bikes-project/ingestion_flows` directory and run:
```
python it-deploy-web-to-gcs-to-bq.py
```
  * This will create and deploy the Docker Image of your ingestion process
  * The deployment should appear in your list of Deployments on Prefect UI
  * Make sure the output of this command has the instructions about how to start the Worker and execute the new Deployment (see next steps)
3. In a different terminal tab start the Prefect Worker:
```
prefect worker start --pool 'my-work-pool'
```
  * Please refer to the Prefect [documentation](https://docs.prefect.io/3.0/deploy/infrastructure-concepts/workers) for more information on Workers
4. In a different terminal tab run the Prefect Deployment:
```
prefect deployment run 'etl-web-to-gcs-to-bq/etl-citybike-web-to-gcs-to-bq-flow-deployment'
```
5. In the Prefect Workers tab check the progress of the ingestion. It should finish error-free and with a message:
```
01:18:31.291 | INFO    | prefect.worker.docker.dockerworker 65b47e53-0c81-4196-9129-571cd9a715e0 - Docker container 'adamant-dormouse' has status 'exited'
```
 * After this, both tables (`networks` and `bike_station_data`) should be built and appear in BigQuery Studio under schema [whatever you have declared as dataset in the profiles.yml file]
6. If everything has finished without issues, navigate to the dbt folder of this repo and run:
```
dbt duild
```
  * This will build and run tests against all the new dbt models in the **dev** environment: `bike_station_with_network_data`, `companies`.
7. If you have a production database too, you can also run
```
dbt build --target prod
```
* This will build and run tests against all the new dbt models this time in the **prod** environment.
    
