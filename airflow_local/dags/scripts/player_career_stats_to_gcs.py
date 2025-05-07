# airflow_local/dags/scripts/player_career_stats_to_gcs.py

import pandas as pd
import time
import requests
import io # For in-memory byte streams

from nba_api.stats.endpoints import commonplayerinfo
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.cloud.exceptions import NotFound # Specific exception for GCS object not found

# This is the core logic for fetching and appending data.
# It's the same as before, taking DataFrames as input.
def append_missing_player_data_logic(df_player_master_list, current_player_data_df, players_to_fetch_count=100):
    existing_player_ids_in_data = pd.Series(dtype='int64')
    if 'player_id' in current_player_data_df.columns and not current_player_data_df.empty:
        existing_player_ids_in_data = current_player_data_df['player_id']
    
    missing_ids_series = df_player_master_list[~df_player_master_list['id'].isin(existing_player_ids_in_data)]['id']
    
    if missing_ids_series.empty:
        print("GCS_SCRIPT_LOGIC: No missing players found based on the master list.")
        return current_player_data_df
        
    ids_to_process_this_run = missing_ids_series.head(players_to_fetch_count)
    
    if ids_to_process_this_run.empty:
        print("GCS_SCRIPT_LOGIC: No players selected for fetching in this batch.")
        return current_player_data_df

    print(f"GCS_SCRIPT_LOGIC: Attempting to fetch data for {len(ids_to_process_this_run)} player IDs: {ids_to_process_this_run.tolist()}")
    newly_fetched_player_info_list = []

    for player_id_to_fetch in ids_to_process_this_run:
        try:
            print(f"GCS_SCRIPT_LOGIC: Fetching info for player ID: {player_id_to_fetch}...")
            player_info_endpoint = commonplayerinfo.CommonPlayerInfo(
                player_id=player_id_to_fetch,
                timeout=10 
            )
            player_df_from_api = player_info_endpoint.common_player_info.get_data_frame()
            
            if player_df_from_api.empty:
                print(f"GCS_SCRIPT_LOGIC: No data returned from API for player ID: {player_id_to_fetch}")
                time.sleep(0.5) 
                continue

            player_df_from_api['player_id'] = player_id_to_fetch 
            newly_fetched_player_info_list.append(player_df_from_api)
            print(f"GCS_SCRIPT_LOGIC: Successfully retrieved info for player ID: {player_id_to_fetch}")
        except requests.exceptions.ReadTimeout:
            print(f"GCS_SCRIPT_LOGIC: ReadTimeoutError for player ID: {player_id_to_fetch}. Stopping fetching for this run.")
            break 
        except Exception as e:
            print(f"GCS_SCRIPT_LOGIC: Error retrieving info for player ID: {player_id_to_fetch}: {type(e).__name__} - {e}")
        time.sleep(1) 

    if newly_fetched_player_info_list:
        new_data_batch_df = pd.concat(newly_fetched_player_info_list, ignore_index=True)
        updated_df = pd.concat([current_player_data_df, new_data_batch_df], ignore_index=True).reset_index(drop=True)
        print(f"GCS_SCRIPT_LOGIC: Appended data for {len(newly_fetched_player_info_list)} players.")
        return updated_df
    else:
        print("GCS_SCRIPT_LOGIC: No new player data was fetched in this run.")
        return current_player_data_df

# Main function for this GCS-centric script
def run_player_extraction_and_upload_to_gcs(
    gcp_conn_id: str,
    gcs_bucket_name: str,
    master_list_gcs_object: str,    # e.g., "nba_config/players.csv"
    player_data_gcs_object: str,    # e.g., "nba_data_accumulated/players_full.parquet"
    players_to_fetch_batch_size: int = 100
):
    """
    Main function for GCS operations:
    1. Reads master player list (CSV) from GCS.
    2. Reads current accumulated player data (Parquet) from GCS.
    3. Fetches data for a batch of missing players using nba_api.
    4. Uploads updated accumulated player data (Parquet) back to GCS.
    """
    print(f"GCS SCRIPT: Starting. Target GCS Bucket: {gcs_bucket_name}")
    gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)

    # 1. Load master player list from GCS (CSV)
    try:
        print(f"GCS SCRIPT: Downloading master player list from gs://{gcs_bucket_name}/{master_list_gcs_object}")
        master_list_bytes = gcs_hook.download(bucket_name=gcs_bucket_name, object_name=master_list_gcs_object)
        df_player_master = pd.read_csv(io.BytesIO(master_list_bytes))
        if 'id' not in df_player_master.columns:
            error_msg = "GCS SCRIPT: Master player list in GCS ('id' column missing)."
            print(error_msg)
            raise ValueError(error_msg)
        print(f"GCS SCRIPT: Loaded master player list. Total players: {len(df_player_master)}")
    except NotFound:
        error_msg = f"GCS SCRIPT: CRITICAL - Master player list 'gs://{gcs_bucket_name}/{master_list_gcs_object}' not found."
        print(error_msg)
        raise 
    except Exception as e:
        error_msg = f"GCS SCRIPT: CRITICAL - Could not read master player list from GCS: {e}"
        print(error_msg)
        raise

    # 2. Load existing accumulated player data from GCS (Parquet)
    expected_player_data_cols = ['PERSON_ID', 'FIRST_NAME', 'LAST_NAME', 'DISPLAY_FIRST_LAST', 'DISPLAY_LAST_COMMA_FIRST', 'DISPLAY_FI_LAST', 'PLAYER_SLUG', 'BIRTHDATE', 'SCHOOL', 'COUNTRY', 'LAST_AFFILIATION', 'HEIGHT', 'WEIGHT', 'SEASON_EXP', 'JERSEY', 'POSITION', 'ROSTERSTATUS', 'GAMES_PLAYED_CURRENT_SEASON_FLAG', 'TEAM_ID', 'TEAM_NAME', 'TEAM_ABBREVIATION', 'TEAM_CODE', 'TEAM_CITY', 'PLAYERCODE', 'FROM_YEAR', 'TO_YEAR', 'DLEAGUE_FLAG', 'NBA_FLAG', 'GAMES_PLAYED_FLAG', 'DRAFT_YEAR', 'DRAFT_ROUND', 'DRAFT_NUMBER', 'GREATEST_75_FLAG', 'player_id']
    player_data_df_current = pd.DataFrame(columns=expected_player_data_cols) # Initialize with schema

    try:
        if gcs_hook.exists(bucket_name=gcs_bucket_name, object_name=player_data_gcs_object):
            print(f"GCS SCRIPT: Downloading existing player data from gs://{gcs_bucket_name}/{player_data_gcs_object}")
            player_data_bytes = gcs_hook.download(bucket_name=gcs_bucket_name, object_name=player_data_gcs_object)
            if player_data_bytes and len(player_data_bytes) > 0: # Check if downloaded bytes are not empty
                player_data_df_current = pd.read_parquet(io.BytesIO(player_data_bytes))
                print(f"GCS SCRIPT: Loaded existing player data. Rows: {len(player_data_df_current)}")
                # Ensure columns match expected if it was an empty Parquet file with schema
                if player_data_df_current.empty and not all(col in player_data_df_current.columns for col in ['player_id']): # Basic check
                     player_data_df_current = pd.DataFrame(columns=expected_player_data_cols)
            else: # File existed but was empty
                print(f"GCS SCRIPT: Player data file 'gs://{gcs_bucket_name}/{player_data_gcs_object}' is empty. Starting with schema.")
        else: # File not found
            print(f"GCS SCRIPT: Player data file 'gs://{gcs_bucket_name}/{player_data_gcs_object}' not found. Starting fresh with schema.")
    except Exception as e:
        print(f"GCS SCRIPT: Error reading existing player data from GCS: {e}. Starting fresh with schema.")
    
    # Ensure player_data_df_current has the expected columns, even if it was loaded empty.
    # This helps pd.concat behave correctly if new data comes in.
    if player_data_df_current.empty:
        player_data_df_current = pd.DataFrame(columns=expected_player_data_cols)


    # 3. Fetch missing data
    updated_player_data_df = append_missing_player_data_logic(
        df_player_master.copy(),
        player_data_df_current.copy(), 
        players_to_fetch_count=players_to_fetch_batch_size
    )

    # 4. Upload the updated DataFrame to GCS as Parquet, only if it has data
    if not updated_player_data_df.empty:
        try:
            parquet_buffer = io.BytesIO()
            updated_player_data_df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
            parquet_buffer.seek(0)

            gcs_hook.upload(
                bucket_name=gcs_bucket_name,
                object_name=player_data_gcs_object, # Overwrite
                data=parquet_buffer.getvalue(),
                mime_type='application/octet-stream'
            )
            print(f"GCS SCRIPT: Successfully uploaded data to gs://{gcs_bucket_name}/{player_data_gcs_object}. Total rows: {len(updated_player_data_df)}")
        except Exception as e:
            error_msg = f"GCS SCRIPT: ERROR - Could not upload player data to GCS: {e}"
            print(error_msg)
            raise
    else:
        print("GCS SCRIPT: No data in updated_player_data_df to upload.")

# The if __name__ == "__main__": block for local testing is omitted here
# as it would require GCS credentials and setup to run meaningfully outside Airflow.