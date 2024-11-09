FROM prefecthq/prefect:2-latest
 
RUN pip install pandas sqlalchemy psycopg2-binary pyarrow "numpy<2" prefect

WORKDIR /app
COPY import_citybikes_data_into_postgres.py import_citybikes_data_into_postgres.py


ENTRYPOINT [ "python", "import_citybikes_data_into_postgres.py" ]
