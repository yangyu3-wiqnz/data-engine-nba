# Dockerfile

# ARG for the FROM instruction itself.
# The value comes from the 'args' section in docker-compose.yaml, which reads from .env
ARG AIRFLOW_IMAGE_NAME="apache/airflow:3.0.0"

# Start the build stage from the base Airflow image
FROM ${AIRFLOW_IMAGE_NAME}

# Re-declare ARGs needed within this build stage (after FROM).
# Their values will be supplied by the 'args' in your docker-compose.yaml.
ARG AIRFLOW_UID
ARG AIRFLOW_GID

# Temporarily switch to root to create directories and set permissions
USER root

# Create the gcp-credentials directory and set its ownership.
# The echo command is for debugging to confirm the UIDs and GIDs being used.
RUN echo "Preparing to set ownership for /opt/airflow/gcp-credentials with UID: '${AIRFLOW_UID}' and GID: '${AIRFLOW_GID}'" && \
    mkdir -p /opt/airflow/gcp-credentials && \
    chown -R "${AIRFLOW_UID}":"${AIRFLOW_GID}" /opt/airflow/gcp-credentials

# Switch to the airflow user (UID specified by AIRFLOW_UID).
# This user should already exist in the official Airflow base image.
USER ${AIRFLOW_UID}

# Copy the requirements.txt file to a temporary location in the container
# and set its ownership to the airflow user.
COPY --chown=${AIRFLOW_UID}:${AIRFLOW_GID} requirements.txt /tmp/requirements.txt

# Install Python dependencies from requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Optional: Clean up the requirements file if desired
# RUN rm /tmp/requirements.txt