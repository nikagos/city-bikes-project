FROM prefecthq/prefect:2-latest

# Set working directory
WORKDIR /opt/prefect/flows

# Copy the requirements.txt file into the container
COPY requirements.txt .

# Install dependencies
RUN pip install -r requirements.txt

# Copy the flow script to the container
COPY etl_web_to_gcs_to_bq.py .

# Set the entry point to start the flow using Prefect
ENTRYPOINT [ "python", "etl_web_to_gcs_to_bq.py" ]
