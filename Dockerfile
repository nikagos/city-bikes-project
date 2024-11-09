# FROM prefecthq/prefect:2-latest
# RUN pip install pandas sqlalchemy psycopg2-binary pyarrow "numpy<2" prefect
# COPY import_citybikes_data_into_postgres.py /opt/prefect/flows/import_citybikes_data_into_postgres.py
# ENTRYPOINT [ "python", "import_citybikes_data_into_postgres.py" ]

FROM prefecthq/prefect:2-latest

# Set working directory
WORKDIR /opt/prefect/flows

# Install dependencies
RUN pip install pandas sqlalchemy psycopg2-binary pyarrow "numpy<2" prefect prefect_sqlalchemy

# Copy the flow script to the container
COPY import_citybikes_data_into_postgres.py .

# Set the entry point to start the flow using Prefect
ENTRYPOINT [ "python", "import_citybikes_data_into_postgres.py" ]
