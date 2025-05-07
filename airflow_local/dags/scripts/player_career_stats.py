# airflow_local/dags/scripts/player_career_stats.py

from nba_api.stats.endpoints import commonplayerinfo
# from nba_api.stats.static import players # Not used in the provided script logic
# from nba_api.stats.static import teams # Not used
import pandas as pd
import time
import requests
import os # For joining paths and checking file existence

# Keep your existing function, maybe rename for clarity if it's part of a larger module
def append_missing_player_data_logic(df_player_master_list, current_player_data_df, players_to_fetch_count=100):
    """
    Retrieves data for a batch of players missing from current_player_data_df
    (based on IDs in df_player_master_list) and returns an updated DataFrame.
    Breaks on ReadTimeoutError.

    Args:
        df_player_master_list (pd.DataFrame): DataFrame with 'id' column for all players.
        current_player_data_df (pd.DataFrame): DataFrame with existing player data, must have 'player_id'.
        players_to_fetch_count (int): Number of missing players to fetch in this batch.

    Returns:
        pd.DataFrame: The updated player data DataFrame.
    """
    # Ensure 'player_id' column exists in current_player_data_df if it's not empty.
    # If current_player_data_df is completely new (empty), it won't have 'player_id' yet.
    existing_player_ids_in_data = pd.Series(dtype='int64')
    if 'player_id' in current_player_data_df.columns:
        existing_player_ids_in_data = current_player_data_df['player_id']
    
    missing_ids_series = df_player_master_list[~df_player_master_list['id'].isin(existing_player_ids_in_data)]['id']
    
    if missing_ids_series.empty:
        print("No missing players found based on the master list.")
        return current_player_data_df
        
    ids_to_process_this_run = missing_ids_series.head(players_to_fetch_count)
    
    if ids_to_process_this_run.empty: # Should be covered by above, but good check
        print("No players selected for fetching in this batch.")
        return current_player_data_df

    print(f"Attempting to fetch data for {len(ids_to_process_this_run)} player IDs: {ids_to_process_this_run.tolist()}")

    newly_fetched_player_info_list = []

    for player_id_to_fetch in ids_to_process_this_run:
        try:
            print(f"Fetching info for player ID: {player_id_to_fetch}...")
            player_info_endpoint = commonplayerinfo.CommonPlayerInfo(
                player_id=player_id_to_fetch,
                timeout=10  # Slightly increased timeout
            )
            # The result is a list of DataFrames, usually we want the first one.
            player_df_from_api = player_info_endpoint.common_player_info.get_data_frame()
            
            if player_df_from_api.empty:
                print(f"No data returned from API for player ID: {player_id_to_fetch}")
                time.sleep(0.5) # Shorter sleep if API returns empty quickly
                continue

            player_df_from_api['player_id'] = player_id_to_fetch  # Add player_id column for consistency
            newly_fetched_player_info_list.append(player_df_from_api)
            print(f"Successfully retrieved info for player ID: {player_id_to_fetch}")

        except requests.exceptions.ReadTimeout:
            print(f"ReadTimeoutError for player ID: {player_id_to_fetch}. Stopping fetching for this run.")
            break  # Break the loop for this batch
        except Exception as e:
            print(f"Error retrieving info for player ID: {player_id_to_fetch}: {type(e).__name__} - {e}")
            # Consider whether to break or continue for other errors
        
        time.sleep(1) # Be respectful to the API between calls

    if newly_fetched_player_info_list:
        new_data_batch_df = pd.concat(newly_fetched_player_info_list, ignore_index=True)
        # Ensure columns align before concat if current_player_data_df could be truly empty initially
        if current_player_data_df.empty and not new_data_batch_df.empty:
             # If starting fresh, the new data is the only data
             updated_df = new_data_batch_df
        elif not new_data_batch_df.empty:
             updated_df = pd.concat([current_player_data_df, new_data_batch_df], ignore_index=True)
        else: # No new data fetched
            updated_df = current_player_data_df
        print(f"Appended data for {len(newly_fetched_player_info_list)} players.")
        return updated_df
    else:
        print("No new player data was fetched in this run.")
        return current_player_data_df


def run_player_extraction_and_save(base_datasets_dir: str, 
                                   master_list_filename: str = "players.csv", 
                                   data_accumulation_filename: str = "players_full.csv"):
    """
    Main callable function for Airflow.
    Reads master player list, reads accumulated data, fetches missing players,
    and saves the updated accumulated data.
    """
    print(f"Script execution started. Base datasets directory: {base_datasets_dir}")

    master_player_list_path = os.path.join(base_datasets_dir, master_list_filename)
    player_data_accumulation_path = os.path.join(base_datasets_dir, data_accumulation_filename)

    # 1. Load master player list
    try:
        df_player_master = pd.read_csv(master_player_list_path)
        if 'id' not in df_player_master.columns:
            print(f"ERROR: Master player list '{master_player_list_path}' must contain an 'id' column.")
            return
        print(f"Loaded master player list '{master_player_list_path}'. Total players in master: {len(df_player_master)}")
    except FileNotFoundError:
        print(f"ERROR: Master player list '{master_player_list_path}' not found. Cannot proceed.")
        return # Or raise an AirflowException
    except Exception as e:
        print(f"ERROR: Could not read master player list '{master_player_list_path}': {e}")
        return

    # 2. Load existing accumulated player data (or start fresh)
    player_data_df_current = pd.DataFrame() # Default to empty
    expected_columns = ['PERSON_ID', 'FIRST_NAME', 'LAST_NAME', 'DISPLAY_FIRST_LAST', 'DISPLAY_LAST_COMMA_FIRST', 'DISPLAY_FI_LAST', 'PLAYER_SLUG', 'BIRTHDATE', 'SCHOOL', 'COUNTRY', 'LAST_AFFILIATION', 'HEIGHT', 'WEIGHT', 'SEASON_EXP', 'JERSEY', 'POSITION', 'ROSTERSTATUS', 'GAMES_PLAYED_CURRENT_SEASON_FLAG', 'TEAM_ID', 'TEAM_NAME', 'TEAM_ABBREVIATION', 'TEAM_CODE', 'TEAM_CITY', 'PLAYERCODE', 'FROM_YEAR', 'TO_YEAR', 'DLEAGUE_FLAG', 'NBA_FLAG', 'GAMES_PLAYED_FLAG', 'DRAFT_YEAR', 'DRAFT_ROUND', 'DRAFT_NUMBER', 'GREATEST_75_FLAG', 'player_id']

    if os.path.exists(player_data_accumulation_path):
        try:
            player_data_df_current = pd.read_csv(player_data_accumulation_path)
            print(f"Loaded existing player data from '{player_data_accumulation_path}'. Rows: {len(player_data_df_current)}")
            # Ensure 'player_id' exists, critical for diffing
            if 'player_id' not in player_data_df_current.columns and not player_data_df_current.empty:
                print(f"WARNING: Existing data file '{player_data_accumulation_path}' is missing 'player_id' column. This might lead to issues.")
                # Attempt to create it if possible, or handle as error
                # For now, if it's missing, the diff logic might re-fetch.
            elif player_data_df_current.empty: # File exists but is empty (e.g. only headers or truly empty)
                 player_data_df_current = pd.DataFrame(columns=expected_columns) # Use expected columns for an empty DF
                 print(f"'{player_data_accumulation_path}' is empty. Initialized with expected columns.")

        except pd.errors.EmptyDataError:
            print(f"Player data file '{player_data_accumulation_path}' exists but is empty. Starting fresh.")
            player_data_df_current = pd.DataFrame(columns=expected_columns)
        except Exception as e:
            print(f"ERROR reading existing player data '{player_data_accumulation_path}': {e}. Attempting to start fresh.")
            player_data_df_current = pd.DataFrame(columns=expected_columns)
    else:
        print(f"Player data file '{player_data_accumulation_path}' not found. Starting fresh.")
        player_data_df_current = pd.DataFrame(columns=expected_columns) # Ensure it has columns for first concat


    # 3. Fetch missing data and update the DataFrame
    # Pass copies to avoid modifying original DFs if function has side effects (though this one returns new)
    updated_player_data_df = append_missing_player_data_logic(
        df_player_master.copy(), 
        player_data_df_current.copy(),
        players_to_fetch_count=100 # This matches your script's batch size
    )

    # 4. Save the updated (or original if no changes) DataFrame back to CSV
    try:
        updated_player_data_df.to_csv(player_data_accumulation_path, index=False)
        print(f"Successfully saved updated player data to '{player_data_accumulation_path}'. Total rows: {len(updated_player_data_df)}")
    except Exception as e:
        print(f"ERROR: Could not write updated player data to '{player_data_accumulation_path}': {e}")
        raise # Important to raise error to Airflow if save fails

# This allows the script to be imported as a module and also run directly for testing
if __name__ == "__main__":
    print("Running player_career_stats.py directly for testing...")
    
    # For direct testing, create a 'datasets' folder relative to this script if it doesn't exist
    # If you run `python scripts/player_career_stats.py` from `airflow_local/dags/`
    # then `test_script_dir_path` would be `scripts/datasets`
    # If you run `python player_career_stats.py` from `airflow_local/dags/scripts/`
    # then `test_script_dir_path` would be `datasets`
    
    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    test_datasets_path = os.path.join(current_script_dir, "..", "..", "datasets") # Assumes script is in dags/scripts and datasets is at airflow_local/datasets
    test_datasets_path = os.path.normpath(test_datasets_path) # Normalize path (e.g., handle '..')

    if not os.path.exists(test_datasets_path):
        os.makedirs(test_datasets_path)
        print(f"Created directory for testing: {test_datasets_path}")

    master_filename = "players.csv"
    data_filename = "players_full.csv"

    # Create a dummy players.csv for testing if it doesn't exist in the target test_datasets_path
    dummy_master_file_path = os.path.join(test_datasets_path, master_filename)
    if not os.path.exists(dummy_master_file_path):
        print(f"Creating dummy '{master_filename}' at '{dummy_master_file_path}' for testing.")
        # A few valid player IDs for testing
        dummy_players_data = {'id': [2544, 201939, 203999, 203507, 1629029, 1628369, 203076, 201142, 201566, 202681, 203954, 201942, 76001, 76002, 76003]} # Added more for testing multiple batches
        pd.DataFrame(dummy_players_data).to_csv(dummy_master_file_path, index=False)
    
    print(f"Running main extraction logic with base_datasets_dir='{test_datasets_path}'...")
    run_player_extraction_and_save(
        base_datasets_dir=test_datasets_path,
        master_list_filename=master_filename,
        data_accumulation_filename=data_filename
    )
    print("Finished direct script test run.")