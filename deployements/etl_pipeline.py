from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule

if __name__ == "__main__":
    deployment = Deployment.build_from_flow(
        flow=etl_pipeline,
        name="prod-deployment",
        work_pool_name="default-agent-pool",
        schedule=CronSchedule(cron="0 2 * * *", timezone="UTC"),
        tags=["prod"]
    )
    deployment.apply()