from prefect.deployments import Deployment
from etl_web_to_gcs import etl_web_to_gcs
from prefect.orion.schemas.schedules import CronSchedule


q2_deployment = Deployment.build_from_flow(
    flow=etl_web_to_gcs,
    name="question-2-flow",
    schedule=CronSchedule(cron="0 5 1 * *", timezone="Etc/UTC")
)


if __name__ == "__main__":
    q2_deployment.apply()