import os
from prefect import flow
from prefect.deployments.runner import DockerImage
from import_citybikes_data_into_postgres import etl_web_to_postgres

user = os.getenv("USER")
github_user = os.getenv("GITHUB_USER")

@flow(log_prints=True)
def my_flow():
    etl_web_to_postgres()


if __name__ == "__main__":
    """This dockerizes the ingestion script using a Dockerfile and creates a Prefect Deployment"""

    my_flow.from_source(
        source=f"git@github.com:{github_user}/city-bikes-project.git",
        entrypoint="import_citybikes_data_into_postgres.py:etl_web_to_postgres"
    ).deploy(
        name="etl-citybike-web-to-postgres-flow-deployment",
        work_pool_name="my-work-pool",
        image=DockerImage(
            name=f"{github_user}/my-citybike-etl-web-to-postgres-flow-image",
            tag="v001",
            dockerfile=f"/home/{user}/city-bikes-project/Dockerfile"
        ),
        push=True
    )