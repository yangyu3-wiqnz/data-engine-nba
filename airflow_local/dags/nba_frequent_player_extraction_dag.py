# dags/nba_frequent_player_extraction_dag.py

from scripts.player_career_stats import run_player_extraction_and_save
# ... rest of your DAG

import pendulum
import os # Not strictly needed here anymore as path logic is in the script
from airflow.decorators import dag, task

# Assuming player_career_stats.py is in airflow_local/dags/scripts/
# and airflow_local/dags/scripts/__init__.py exists
from scripts.player_career_stats import run_player_extraction_and_save # <--- Import the main function

# This is the path INSIDE the Airflow container.
# It's mapped to './datasets' on your host via docker-compose.yaml.
CONTAINER_DATASETS_DIR_PATH = "/opt/airflow/datasets"
MASTER_PLAYER_CSV_FILENAME = "players.csv"       # Name of your master list CSV
PLAYER_DATA_CSV_FILENAME = "players_full.csv"  # Name of your accumulating data CSV

@dag(
    dag_id='nba_extract_10_players_script_v1', # Updated DAG ID for clarity
    schedule='*/3 * * * *',  # Cron expression: "at every 3rd minute"
    start_date=pendulum.datetime(2025, 5, 7, tz="Pacific/Auckland"), 
    catchup=False,
    max_active_runs=1,
    tags=['nba', 'frequent-extraction', 'scripted', 'local-save'],
    doc_md="""
    ### NBA Player Data - Frequent Extraction (Scripted v1)
    This DAG runs a Python script located in `dags/scripts/player_career_stats.py` every 3 minutes.
    The script reads a master list of players (`players.csv`), fetches data for up to 10 missing players
    using the nba_api, and updates/saves the accumulated player data (`players_full.csv`)
    in the `/opt/airflow/datasets` directory (mapped from the host's `./datasets` folder).

    **Prerequisites on Host:**
    - `./datasets/players.csv` must exist and contain a column 'id' with player IDs.
    - `./datasets/players_full.csv` will be created or updated by the script.
    """
)
def frequent_nba_player_data_extraction_scripted_dag():

    @task
    def execute_player_stats_script():
        """
        Airflow task that calls the main function from the player_career_stats.py script.
        """
        print(f"Task started: Calling 'run_player_extraction_and_save' from script.")
        try:
            run_player_extraction_and_save(
                base_datasets_dir=CONTAINER_DATASETS_DIR_PATH,
                master_list_filename=MASTER_PLAYER_CSV_FILENAME,
                data_accumulation_filename=PLAYER_DATA_CSV_FILENAME
            )
            print("'run_player_extraction_and_save' executed successfully by Airflow task.")
        except Exception as e:
            print(f"ERROR: The Airflow task caught an exception from 'run_player_extraction_and_save': {type(e).__name__} - {e}")
            raise # Re-raise the exception to make the Airflow task fail

    execute_player_stats_script()

# Instantiate the DAG
dag_instance_scripted_v1 = frequent_nba_player_data_extraction_scripted_dag()