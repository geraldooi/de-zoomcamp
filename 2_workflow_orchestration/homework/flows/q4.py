from prefect.deployments import Deployment
from prefect.filesystems import GitHub
from prefect.infrastructure.docker import DockerContainer
from q1 import etl_web_to_gcs


docker_container_block = DockerContainer.load("prefect")

github_block = GitHub.load("de-zoomcamp")


if __name__ == "__main__":
    q4_deployment = Deployment.build_from_flow(
        flow=etl_web_to_gcs,
        name="question-4-flow",
        storage=github_block,
        path='',
        entrypoint="2_workflow_orchestration/homework/flows/q1.py:etl_web_to_gcs"
    )
    q4_deployment.apply()