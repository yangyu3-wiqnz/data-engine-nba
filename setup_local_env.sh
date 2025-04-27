#!/bin/bash

# Set the Python virtual environment name
VENV_DIR="venv"
DBT_PROFILES_DIR="$HOME/.dbt"
SOURCE_PROFILE="analytics_dbt_starter/profiles_example/profiles.yml"
PROJECT_ID="gcp-wow-cd-email-app-test"
REPO_DIR="wwnz-ai-analytical-project-template"
EXPIRATION_SEC=$((14 * 24 * 60 * 60))  # 2 weeks in seconds
TIME_TRAVEL_HOURS=48  # 2 days in hours

# Ensure the user is in the correct folder
CURRENT_DIR=$(basename "$PWD")

if [ "$CURRENT_DIR" != "$REPO_DIR" ]; then
    echo "Error: This script must be run from inside the '$REPO_DIR' directory."
    echo "Current directory: $CURRENT_DIR"
    exit 1
fi

echo "Script is running inside the correct directory: $REPO_DIR"

# Ensure Cloud Shell has Python installed
if ! command -v python3 &> /dev/null; then
    echo "Python3 is not installed. Please install it first."
    exit 1
fi

# Create a virtual environment if it doesn't exist
if [ ! -d "$VENV_DIR" ]; then
    echo "Creating virtual environment..."
    python3 -m venv "$VENV_DIR"
else
    echo "Virtual environment already exists."
fi

# Activate the virtual environment
source "$VENV_DIR/bin/activate"

# Upgrade pip
pip install --upgrade pip

# Check if requirements.txt exists
if [ -f "requirements.txt" ]; then
    echo "Installing dependencies from requirements.txt..."
    pip install -r requirements.txt
else
    echo "requirements.txt not found. Skipping dependency installation."
fi

# Install pre-commit
echo "Installing pre-commit..."
pip install pre-commit
pre-commit migrate-config

# Initialize pre-commit if a .pre-commit-config.yaml file exists
if [ -f ".pre-commit-config.yaml" ]; then
    echo "Setting up pre-commit hooks..."
    pre-commit install
else
    echo "No .pre-commit-config.yaml found. Skipping hook installation."
fi

echo "Virtual environment setup complete."

# Get current GCP username and extract short username
USER_EMAIL=$(gcloud auth list --filter=status:ACTIVE --format="value(account)")
SHORT_USERNAME=$(echo "$USER_EMAIL" | cut -d'@' -f1)
# Remove special characters from username (keep only a-z, A-Z, 0-9, and _)
SANITIZED_USERNAME=$(echo "$SHORT_USERNAME" | tr -cd 'a-zA-Z0-9_')

echo "Username: $SANITIZED_USERNAME"

DBT_DATASET="dbt_dev__$SANITIZED_USERNAME"

if [ ! -d "$DBT_PROFILES_DIR" ]; then
    echo "Creating DBT profiles directory at $DBT_PROFILES_DIR"
    mkdir -p "$DBT_PROFILES_DIR"
    sed "9s|dataset: .*|dataset: $DBT_DATASET|" "$SOURCE_PROFILE" > $DBT_PROFILES_DIR/profiles.yml
fi

if bq --project_id="$PROJECT_ID" show --format=none "$DBT_DATASET" 2>/dev/null; then
    echo "Dataset $DBT_DATASET already exists in project $PROJECT_ID. Skipping creation."
else
    echo "Creating dataset $DBT_DATASET in project $PROJECT_ID..."
    bq --project_id="$PROJECT_ID" mk --dataset \
        --description="DBT development dataset for $USER_EMAIL" \
        --default_table_expiration "$EXPIRATION_SEC" \
        --default_partition_expiration "$EXPIRATION_SEC" \
        --max_time_travel_hours "$TIME_TRAVEL_HOURS" \
        "$DBT_DATASET"
fi
echo "dbt setup complete"
