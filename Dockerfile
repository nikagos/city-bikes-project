FROM prefecthq/prefect:2-latest

# Set working directory
WORKDIR /opt/prefect/flows

# Install dependencies
RUN pip install -r requirements.txt

# Copy the flow script to the container
COPY import_citybikes_data_into_postgres.py .

# Set the entry point to start the flow using Prefect
ENTRYPOINT [ "python", "import_citybikes_data_into_postgres.py" ]
