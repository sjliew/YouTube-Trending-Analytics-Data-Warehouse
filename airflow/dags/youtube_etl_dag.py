from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import datetime

with DAG(
    "youtube_etl_pipeline",
    start_date=datetime(2025, 11, 16),
    schedule_interval="@daily",
    catchup=False
) as dag:

    dim_video = GlueJobOperator(
        task_id="dim_video",
        job_name="dim_video_etl_job"
    )

    dim_channel = GlueJobOperator(
        task_id="dim_channel",
        job_name="dim_channel_etl_job"
    )

    dim_category = GlueJobOperator(
        task_id="dim_category",
        job_name="dim_category_etet_job"
    )

    dim_country = GlueJobOperator(
        task_id="dim_country",
        job_name="dim_country_etl_job"
    )

    fact_video_stats = GlueJobOperator(
        task_id="fact_video_stats",
        job_name="fact_video_stats_etl_job"
    )

    fact_engagement = GlueJobOperator(
        task_id="fact_engagement",
        job_name="fact_engagement_etl_job"
    )

    fact_trending_duration = GlueJobOperator(
        task_id="fact_trending_duration",
        job_name="fact_trending_duration_etl_job"
    )

    [dim_video, dim_channel, dim_category, dim_country] >> fact_video_stats >> [fact_engagement, fact_trending_duration]
