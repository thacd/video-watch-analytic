import datetime

from airflow import models
from airflow.contrib.operators.dataflow_operator \
    import DataflowTemplateOperator
from airflow.utils.dates import days_ago

bucket_path = models.Variable.get("bucket_path")
project_id = models.Variable.get("project_id")
gce_zone = models.Variable.get("gce_zone")
gce_region = models.Variable.get("gce_region")


default_args = {
    # Tell airflow to start one day ago, so that it runs as soon as you upload
    "start_date": days_ago(1),
    "dataflow_default_options": {
        "project": project_id,
        # Set to your region
        "region": gce_region,
        # Set to your zone
        "zone": gce_zone,
        # This is a subfolder for storing temporary files.
        "temp_location": bucket_path + "/tmp/",
    },
}

# Define a DAG (directed acyclic graph) of tasks.
# Any task you create within the context manager is automatically added to the
# DAG object.
with models.DAG(
    # The id you will see in the DAG airflow page
    "composer_dataflow_dag",
    default_args=default_args,
    # The interval with which to schedule the DAG
    schedule_interval=datetime.timedelta(days=1),
) as dag:

    start_template_job = DataflowTemplateOperator(
        # The task id of your job
        task_id="dataflow_operator_import_and_transform_csv_to_bq",
        # The name of the template that you're using.
        template="gs://video_watch/pipelines/data_transformation",
        parameters={
            "update": bucket_path + "/data/sample.csv"
        },
    )
