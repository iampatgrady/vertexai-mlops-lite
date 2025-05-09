# vai-basic/Dockerfile
# Use the same base Python version
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Copy requirements file first to leverage Docker layer caching
COPY requirements.txt .

# Install dependencies
# Use --no-cache-dir to reduce image size
# Upgrade pip first
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy the 'pipelines' directory into the image at /app/pipelines
# This makes your component code (e.g., pipelines/components/bigquery_components.py)
# and any supporting modules within 'pipelines' (like pipelines/__init__.py if imported)
# available to the KFP executor running inside this container.
COPY ./pipelines ./pipelines
# ------------------------

# The KFP SDK handles the entrypoint for components,
# so no explicit CMD or ENTRYPOINT is typically needed here for the component base image.
