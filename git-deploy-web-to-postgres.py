from prefect import flow
from prefect.deployments.runner import DockerImage
from import_citybikes_data_into_postgres import etl_web_to_postgres


@flow(log_prints=True)
def my_flow():
    etl_web_to_postgres()


if __name__ == "__main__":
    my_flow.from_source(
        source="git@github.com:nikagos/city-bikes-project.git",
        entrypoint="import_citybikes_data_into_postgres.py:etl_web_to_postgres"
    ).deploy(
        name="etl-citybike-web-to-postgres-flow-deployment",
        work_pool_name="my-work-pool",
        image=DockerImage(
            name="nikagos/my-citybike-etl-web-to-postgres-flow-image",
            tag="v001",
            dockerfile="/home/nickk/city-bikes-project/Dockerfile"
        ),
        push=True
    )