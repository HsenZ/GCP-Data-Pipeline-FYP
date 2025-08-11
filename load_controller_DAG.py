from airflow import models
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.dates import days_ago

def decide_branch(**kwargs):
    filename = kwargs['dag_run'].conf.get('filename', '')
    if filename.startswith("whole_month_"):
        return "full-load-staging"
    elif filename.startswith("all_day_"):
        return "delta-load-staging"
    else:
        raise ValueError(f"Unexpected filename format: {filename}")

with models.DAG(
    dag_id="load_pipeline_controller",
    start_date=days_ago(1),
    schedule_interval=None,  # Triggered by Cloud Run only
    catchup=False,
    max_active_runs=1,
    tags=["conditional", "dataflow", "controller"],
) as dag:

    branch = BranchPythonOperator(
        task_id="decide_path",
        python_callable=decide_branch,
    )

    # -----------------------------------------------------------------------------------------------------------------------
    # FULL LOAD BRANCH
    FL_job_1 = BashOperator(
        task_id="full-load-staging",
        bash_command="""
        python3 /home/airflow/gcs/data/stg_full_load.py \
          --runner DataflowRunner \
          --project imp-fyp \
          --region us-central1 \
          --worker_machine_type=e2-standard-4 \
          --max_num_workers=2 \
          --temp_location gs://dataflow-intermediate-bucket/temp \
          --staging_location gs://dataflow-intermediate-bucket/staging \
          --job_name full-load-staging
        """
    )

    FL_job_2 = BashOperator(
        task_id="full-load-ods-without-country-parsing",
        bash_command="""
        python3 /home/airflow/gcs/data/ods_full_load2.py \
          --runner DataflowRunner \
          --project imp-fyp \
          --region us-central1 \
          --worker_machine_type=e2-standard-4 \
          --max_num_workers=2 \
          --temp_location gs://dataflow-intermediate-bucket/temp \
          --staging_location gs://dataflow-intermediate-bucket/staging \
          --job_name ods-full-load
        """
    )

    FL_job_3 = BashOperator(
        task_id="full-load-ods-country-parsing",
        bash_command="""
        python3 /home/airflow/gcs/data/parse_country_ods_full_load2.py \
          --runner DataflowRunner \
          --project imp-fyp \
          --region us-central1 \
          --worker_machine_type=e2-standard-4 \
          --max_num_workers=2 \
          --temp_location gs://dataflow-intermediate-bucket/temp \
          --staging_location gs://dataflow-intermediate-bucket/staging \
          --job_name parse-country-ods-full-load
        """
    )

    FL_job_4 = BashOperator(
        task_id='full-load-dw',
        bash_command="""
        python3 /home/airflow/gcs/data/dw_full_load2.py \
          --runner DataflowRunner \
          --project imp-fyp \
          --region us-central1 \
          --worker_machine_type=e2-standard-4 \
          --max_num_workers=2 \
          --temp_location gs://dataflow-intermediate-bucket/temp \
          --staging_location gs://dataflow-intermediate-bucket/staging \
          --job_name dw-full-load \
          --save_main_session
        """
    )

    FL_job_5 = BashOperator(
        task_id='full-load-move-raw-csv-to-archive',
        bash_command="""
       python3 /home/airflow/gcs/data/move_to_archive_full_load.py \
        --runner DataflowRunner \
        --project imp-fyp \
        --region us-central1 \
        --worker_machine_type=e2-standard-4 \
        --max_num_workers=2 \
        --temp_location gs://dataflow-intermediate-bucket/temp \
        --staging_location gs://dataflow-intermediate-bucket/staging \
        --job_name move-to-archive-full
        """
    )

    # -----------------------------------------------------------------------------------------------------------------------
    # DELTA LOAD BRANCH
    DL_job_1 = BashOperator(
        task_id='delta-load-staging',
        bash_command="""
        python3 /home/airflow/gcs/data/stg_delta_load.py \
          --runner DataflowRunner \
          --project imp-fyp \
          --region us-central1 \
          --worker_machine_type=e2-standard-4 \
          --max_num_workers=2 \
          --temp_location gs://dataflow-intermediate-bucket/temp \
          --staging_location gs://dataflow-intermediate-bucket/staging \
          --job_name stg-delta-load
        """
    )

    DL_job_2 = BashOperator(
        task_id='delta-ods-without-country-parsing',
        bash_command="""
        python3 /home/airflow/gcs/data/ods_delta_load2.py \
          --runner DataflowRunner \
          --project imp-fyp \
          --region us-central1 \
          --worker_machine_type=e2-standard-4 \
          --max_num_workers=2 \
          --temp_location gs://dataflow-intermediate-bucket/temp \
          --staging_location gs://dataflow-intermediate-bucket/staging \
          --job_name ods-delta-load
        """
    )

    DL_job_3 = BashOperator(
        task_id='delta-ods-country-parsing',
        bash_command="""
        python3 /home/airflow/gcs/data/parse_country_ods_delta_load2.py \
          --runner DataflowRunner \
          --project imp-fyp \
          --region us-central1 \
          --worker_machine_type=e2-standard-4 \
          --max_num_workers=2 \
          --temp_location gs://dataflow-intermediate-bucket/temp \
          --staging_location gs://dataflow-intermediate-bucket/staging \
          --job_name parse-country-ods-delta-load
        """
    )

    DL_job_4 = BashOperator(
        task_id='delta-dw',
        bash_command="""
        python3 /home/airflow/gcs/data/dw2_delta_load2.py \
          --runner DataflowRunner \
          --project imp-fyp \
          --region us-central1 \
          --worker_machine_type=e2-standard-4 \
          --max_num_workers=2 \
          --temp_location gs://dataflow-intermediate-bucket/temp \
          --staging_location gs://dataflow-intermediate-bucket/staging \
          --job_name dw-delta-load \
          --save_main_session
        """
    )

    DL_job_5 = BashOperator(
        task_id='delta-load-move-raw-csv-to-archive',
        bash_command="""
       python3 /home/airflow/gcs/data/move_to_archive_delta_load.py \
        --runner DataflowRunner \
        --project imp-fyp \
        --region us-central1 \
        --worker_machine_type=e2-standard-4 \
        --max_num_workers=2 \
        --temp_location gs://dataflow-intermediate-bucket/temp \
        --staging_location gs://dataflow-intermediate-bucket/staging \
        --job_name move-to-archive-delta
        """
    )

    # Branching logic
    branch >> [FL_job_1, DL_job_1]
    FL_job_1 >> FL_job_2 >> FL_job_3 >> FL_job_4 >> FL_job_5
    DL_job_1 >> DL_job_2 >> DL_job_3 >> DL_job_4 >> DL_job_5
