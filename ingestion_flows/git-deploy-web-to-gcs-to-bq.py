import os
from prefect import flow
from prefect.deployments.runner import DockerImage
from etl_web_to_gcs_to_bq import etl_web_to_gcs_to_bq


user = os.getenv("USER")
github_user = os.getenv("GITHUB_USER")

@flow(log_prints=True)
def my_flow():
    etl_web_to_gcs_to_bq()


if __name__ == "__main__":
    """This dockerizes the ingestion script using a Dockerfile and creates a Prefect Deployment"""

    my_flow.from_source(
        source=f"git@github.com:{github_user}/city-bikes-project.git",
        entrypoint="ingestion_flows/etl_web_to_gcs_to_bq.py:etl_web_to_gcs_to_bq"
    ).deploy(
        name="etl-citybike-web-to-gcs-to-bq-flow-deployment",
        work_pool_name="my-work-pool",
        image=DockerImage(
            name=f"{github_user}/my-citybike-etl-web-to-gcs-to-bq-flow-image",
            tag="v001",
            dockerfile=f"/home/{user}/city-bikes-project/ingestion_flows/Dockerfile"
        ),
        push=True
    )