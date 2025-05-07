# airflow_local/dags/nba_frequent_player_extraction_dag_to_gcs.py

import pendulum
from airflow.decorators import dag, task

# Import the GCS-specific main function from your new script
# Make sure airflow_local/dags/scripts/__init__.py exists
from scripts.player_career_stats_to_gcs import run_player_extraction_and_upload_to_gcs

# --- Airflow DAG Configuration ---
GCP_CONNECTION_ID = "gcp-nba-pel-prod"  # Your Airflow GCP connection ID
GCS_BUCKET_NAME = "gcp-nba-offcial-api-data"    # <--- TODO: Replace with YOUR GCS bucket name

# Define paths for your files within the GCS bucket
GCS_MASTER_LIST_OBJECT_PATH = "nba_config/players.csv"
GCS_PLAYER_DATA_OBJECT_PATH = "nba_data_accumulated/players_full.parquet" # Storing as Parquet

@dag(
    dag_id='nba_player_stats_to_gcs_pipeline_v1', # New, unique DAG ID
    schedule='*/3 * * * *',  # Every 3 minutes
    start_date=pendulum.datetime(2025, 5, 7, tz="Pacific/Auckland"), # Adjust as needed
    end_date=pendulum.datetime(2025, 5, 8, 0, 0, 0, tz="Pacific/Auckland"), # Example: Stop on May 8th, 2025, at midnight NZST
    catchup=False,
    max_active_runs=1,
    tags=['nba', 'gcs-pipeline', 'frequent-extraction'],
    doc_md="""
    ### NBA Player Data - Extraction directly to GCS (v1)
    This DAG runs a Python script every 3 minutes to:
    1. Read a master player list (`""" + GCS_MASTER_LIST_OBJECT_PATH + """`) from GCS bucket `""" + GCS_BUCKET_NAME + """`.
    2. Read current accumulated player data (`""" + GCS_PLAYER_DATA_OBJECT_PATH + """`) from the same GCS bucket.
    3. Fetch data for up to 10 missing players using the nba_api.
    4. Upload the updated accumulated player data (as Parquet) back to the GCS bucket.

    **Prerequisites in GCS:**
    - The GCS bucket `""" + GCS_BUCKET_NAME + """` must exist.
    - The master list file `""" + GCS_MASTER_LIST_OBJECT_PATH + """` must exist in the bucket (CSV format, 'id' column).
    - The Airflow GCP connection (`""" + GCP_CONNECTION_ID + """`) must have read/write permissions.
    """
)
def frequent_nba_player_data_to_gcs_dag():

    @task
    def etl_task_fetch_and_upload_to_gcs():
        """
        Airflow task that calls the GCS-centric player data script.
        """
        print(f"DAG Task: Initiating 'run_player_extraction_and_upload_to_gcs'")
        try:
            run_player_extraction_and_upload_to_gcs(
                gcp_conn_id=GCP_CONNECTION_ID,
                gcs_bucket_name=GCS_BUCKET_NAME,
                master_list_gcs_object=GCS_MASTER_LIST_OBJECT_PATH,
                player_data_gcs_object=GCS_PLAYER_DATA_OBJECT_PATH,
                players_to_fetch_batch_size=10
            )
            print("DAG Task: 'run_player_extraction_and_upload_to_gcs' completed.")
        except Exception as e:
            print(f"DAG Task: CRITICAL ERROR during GCS player data script execution: {type(e).__name__} - {e}")
            raise

    etl_task_fetch_and_upload_to_gcs()

# Instantiate the DAG
dag_instance_gcs_pipeline_v1 = frequent_nba_player_data_to_gcs_dag()