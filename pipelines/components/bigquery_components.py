# /vai-basic/pipelines/components/bigquery_components.py
from typing import NamedTuple, Optional, Dict, Tuple
from kfp.dsl import component, Input, Output, Dataset, Model, Artifact
from pipelines import base_image

# --- Components ---

# Component 1: Create Feature Metadata Table (Using Named Parameters)
@component(base_image=base_image)
def create_feature_metadata(
    project: str,
    location: str,
    dataset_id: str,
    analytics_project_id: str,
    analytics_ga4_id: str,
    analytics_ga4_stream_id: str,
    analysis_lookback_days: int,
    top_n_string_values: int,
    pipeline_run_id: str,
) -> NamedTuple('Outputs', [
                           ('feature_metadata_table_id', str),
                           ('feature_mapping_table_id', str)
                           ]):
    from logging import Logger
    import logging
    import re
    import datetime
    from google.cloud import bigquery
    from collections import namedtuple as collections_namedtuple

    def setup_component_logging(logger_name: str) -> Logger:
        logger = logging.getLogger(logger_name)
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', force=True)
        return logger

    logger = setup_component_logging('kfp_component_metadata')
    sanitized_run_id = pipeline_run_id.replace('-', '_')
    if not re.match(r"^[a-zA-Z]", sanitized_run_id):
        sanitized_run_id = f"run_{sanitized_run_id}"
    if sanitized_run_id.startswith('_'):
        sanitized_run_id = f"x{sanitized_run_id}"
    versioned_metadata_table_id = f"{project}.{dataset_id}.feature_metadata_{sanitized_run_id}"
    mapping_table_id = f"{project}.{dataset_id}.feature_value_mapping"
    logger.info(f"Target versioned metadata table: {versioned_metadata_table_id}")
    logger.info(f"Target persistent mapping table: {mapping_table_id}")
    procedure_id = f"{project}.{dataset_id}.create_feature_metadata"
    logger.info(f"Executing BQ Stored Procedure: {procedure_id}")
    client = bigquery.Client(project=project, location=location)
    params = [
        bigquery.ScalarQueryParameter('sp_project_id', "STRING", project),
        bigquery.ScalarQueryParameter('sp_dataset_id', "STRING", dataset_id),
        bigquery.ScalarQueryParameter('analytics_project_id', "STRING", analytics_project_id),
        bigquery.ScalarQueryParameter('analytics_ga4_id', "STRING", analytics_ga4_id),
        bigquery.ScalarQueryParameter('analytics_ga4_stream_id', "STRING", analytics_ga4_stream_id),
        bigquery.ScalarQueryParameter('analysis_lookback_days', "INT64", analysis_lookback_days),
        bigquery.ScalarQueryParameter('top_n_string_values', "INT64", top_n_string_values),
        bigquery.ScalarQueryParameter('target_metadata_table_name', "STRING", versioned_metadata_table_id),
        bigquery.ScalarQueryParameter('target_mapping_table_name', "STRING", mapping_table_id),
    ]
    job_config = bigquery.QueryJobConfig(query_parameters=params, labels={"app": "mini-mlops", "pipeline_step": "feature-analysis-v3", "run_id": pipeline_run_id})
    sql = f"""
        DECLARE created_metadata_table_name STRING;
        DECLARE used_mapping_table_name STRING;
        CALL `{procedure_id}`(
            created_metadata_table_name, used_mapping_table_name,
            @sp_project_id, @sp_dataset_id, @analytics_project_id, @analytics_ga4_id,
            @analytics_ga4_stream_id, @analysis_lookback_days, @top_n_string_values,
            @target_metadata_table_name, @target_mapping_table_name
        );
        SELECT created_metadata_table_name, used_mapping_table_name;
    """
    try:
        query_job = client.query(sql, job_config=job_config, location=location)
        results = query_job.result()
        actual_metadata_table_id = None
        actual_mapping_table_id = None
        for row in results:
            actual_metadata_table_id = row[0]
            actual_mapping_table_id = row[1]
            break
        if not actual_metadata_table_id or not actual_mapping_table_id:
            raise RuntimeError("Stored procedure did not return the created/used table names.")
        logger.info(f"SP call finished. Metadata table: '{actual_metadata_table_id}', Mapping table: '{actual_mapping_table_id}'.")
        Outputs = collections_namedtuple('Outputs', ['feature_metadata_table_id', 'feature_mapping_table_id'])
        return Outputs(actual_metadata_table_id, actual_mapping_table_id)
    except Exception as e:
        logger.error(f"Error calling stored procedure {procedure_id}: {e}")
        if 'query_job' in locals() and hasattr(query_job, 'errors') and query_job.errors: logger.error(f"BQ Job Errors: {query_job.errors}")
        if 'query_job' in locals() and hasattr(query_job, 'error_result') and query_job.error_result: logger.error(f"BQ Error Result: {query_job.error_result}")
        raise

# Component 2: Create Training/Inference Data Table
@component(base_image=base_image)
def create_training_inference_data(
    project: str, location: str, dataset_id: str,
    feature_metadata_table_id: str, analytics_project_id: str, analytics_ga4_id: str,
    analytics_ga4_stream_id: str, mode: str, data_date_start: str, data_date_end: str,
    lookback_days: int, lookahead_days: int, output_dataset: Output[Dataset]
) -> None:
    from logging import Logger
    import logging
    import datetime
    from google.cloud import bigquery
    def setup_component_logging(logger_name: str) -> Logger:
        logger = logging.getLogger(logger_name)
        if not logger.handlers:
            handler = logging.StreamHandler(); formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'); handler.setFormatter(formatter); logger.addHandler(handler)
        logger.setLevel(logging.INFO); logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', force=True); return logger
    logger = setup_component_logging('kfp_component_data_creation')
    metadata_table_full_id = feature_metadata_table_id
    run_timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d%H%M%S")
    target_table_full_id = f"{project}.{dataset_id}.{mode.lower()}_data_{run_timestamp}"
    procedure_id = f"{project}.{dataset_id}.create_training_inference_data"
    client = bigquery.Client(project=project, location=location)
    date_start_obj = datetime.datetime.strptime(data_date_start, '%Y-%m-%d').date()
    date_end_obj = datetime.datetime.strptime(data_date_end, '%Y-%m-%d').date()
    params = [
        bigquery.ScalarQueryParameter(None, "STRING", target_table_full_id),
        bigquery.ScalarQueryParameter(None, "STRING", metadata_table_full_id),
        bigquery.ScalarQueryParameter(None, "STRING", analytics_project_id),
        bigquery.ScalarQueryParameter(None, "STRING", analytics_ga4_id),
        bigquery.ScalarQueryParameter(None, "STRING", analytics_ga4_stream_id),
        bigquery.ScalarQueryParameter(None, "DATE", date_start_obj),
        bigquery.ScalarQueryParameter(None, "DATE", date_end_obj),
        bigquery.ScalarQueryParameter(None, "STRING", mode),
        bigquery.ScalarQueryParameter(None, "INT64", lookback_days),
        bigquery.ScalarQueryParameter(None, "INT64", lookahead_days), ]
    job_config = bigquery.QueryJobConfig(query_parameters=params, labels={"app": "mini-mlops", "pipeline_mode": mode.lower(), "pipeline_step": "data-creation-v3"})
    sql = f"CALL `{procedure_id}`(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"
    try:
        query_job = client.query(sql, job_config=job_config, location=location); query_job.result()
        table = client.get_table(target_table_full_id)
        output_dataset.uri = f"bq://{table.project}.{table.dataset_id}.{table.table_id}"
        output_dataset.metadata = { 'table_id': table.table_id, 'dataset_id': table.dataset_id, 'project_id': table.project, 'location': table.location, 'num_rows': table.num_rows, 'schema': [{'name': f.name, 'type': f.field_type} for f in table.schema], 'target_table_name': target_table_full_id, 'source_feature_metadata_table_id': metadata_table_full_id }
    except Exception as e:
        logger.error(f"Error calling SP {procedure_id}: {e}")
        if 'query_job' in locals() and hasattr(query_job, 'errors') and query_job.errors: logger.error(f"BQ Job Errors: {query_job.errors}")
        if 'query_job' in locals() and hasattr(query_job, 'error_result') and query_job.error_result: logger.error(f"BQ Error Result: {query_job.error_result}")
        raise

# Component 3: Train BQML Model
@component(base_image=base_image)
def train_bqml_model( project: str, location: str, dataset_id: str, model_display_name: str, training_data: Input[Dataset], feature_metadata_table_id: str, model: Output[Model], bqml_options: Dict, ) -> None:
    from logging import Logger
    import logging
    import json
    from google.cloud import bigquery
    def setup_component_logging(logger_name: str) -> Logger:
        logger = logging.getLogger(logger_name)
        if not logger.handlers:
            handler = logging.StreamHandler(); formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'); handler.setFormatter(formatter); logger.addHandler(handler)
        logger.setLevel(logging.INFO); logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', force=True); return logger
    logger = setup_component_logging('kfp_component_train')
    client = bigquery.Client(project=project, location=location); job_config = bigquery.QueryJobConfig()
    training_table_id = f"{training_data.metadata['project_id']}.{training_data.metadata['dataset_id']}.{training_data.metadata['table_id']}"
    model_id = f"{project}.{dataset_id}.{model_display_name}"
    final_options_dict = { 'input_label_cols': ['label'], 'data_split_method': 'CUSTOM', 'data_split_col': 'data_split' }
    final_options_dict.update(bqml_options)
    options_str_list = []
    for key, value in final_options_dict.items():
        if isinstance(value, str): options_str_list.append(f"{key}='{value}'")
        elif isinstance(value, bool): options_str_list.append(f"{key}={str(value).upper()}")
        elif isinstance(value, (int, float)): options_str_list.append(f"{key}={value}")
        elif isinstance(value, list):
            if all(isinstance(item, str) for item in value): quoted_list = [f"'{item}'" for item in value]; options_str_list.append(f"{key}=[{', '.join(quoted_list)}]")
            elif all(isinstance(item, (int, float, bool)) for item in value): options_str_list.append(f"{key}=[{', '.join(map(str, value))}]")
            else: options_str_list.append(f"{key}=['{str(value)}']")
        elif value is None: logger.warning(f"Skipping option '{key}': None.")
        else: options_str_list.append(f"{key}='{str(value)}'")
    options_sql = ",\n        ".join(options_str_list)
    sql = f"""CREATE OR REPLACE MODEL `{model_id}` OPTIONS({options_sql}) AS SELECT * FROM `{training_table_id}`;"""
    try:
        query_job = client.query(sql, job_config=job_config, location=location); query_job.result()
        bq_model_ref = client.get_model(model_id)
        model.uri = f"bq://{model_id.replace(':', '.')}"
        model.metadata = {
            "model_id": bq_model_ref.model_id,
            "dataset_id": bq_model_ref.dataset_id,
            "project_id": bq_model_ref.project,
            "location": bq_model_ref.location,
            "framework": "BQML",
            "model_type": bq_model_ref.model_type,
            "source_feature_metadata_table_id": feature_metadata_table_id # Ensure this is set during training
        }
        if bq_model_ref.training_runs:
             latest_run = bq_model_ref.training_runs[0]; model.metadata["training_started"] = latest_run.get('startTime'); model.metadata["training_completed"] = latest_run.get('endTime'); model.metadata["training_options"] = json.dumps(latest_run.get('trainingOptions', {}))
             if latest_run.get('results'): model.metadata["training_results"] = json.dumps(latest_run.get('results'))
             if latest_run.get('evaluation'): model.metadata["evaluation_metrics"] = json.dumps(latest_run.get('evaluation'))
    except Exception as e:
        logger.error(f"Error training BQML model {model_id}: {e}")
        if 'query_job' in locals() and hasattr(query_job, 'errors') and query_job.errors: logger.error(f"BQ Job Errors: {query_job.errors}")
        if 'query_job' in locals() and hasattr(query_job, 'error_result') and query_job.error_result: logger.error(f"BQ Error Result: {query_job.error_result}")
        raise

# Component 4: Predict using BQML Model
@component(base_image=base_image)
def predict_bqml_model( project: str, location: str, dataset_id: str, model: Input[Model], inference_data: Input[Dataset], predictions: Output[Dataset], prediction_threshold: Optional[float] = None ) -> NamedTuple('Outputs', [ ('predictions_table_full_id', str), ('predictions_count', int) ]):
    from logging import Logger
    import logging
    import time
    from google.cloud import bigquery
    from collections import namedtuple as collections_namedtuple
    def setup_component_logging(logger_name: str) -> Logger:
        logger = logging.getLogger(logger_name)
        if not logger.handlers:
            handler = logging.StreamHandler(); formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'); handler.setFormatter(formatter); logger.addHandler(handler)
        logger.setLevel(logging.INFO); logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', force=True); return logger
    logger = setup_component_logging('kfp_component_predict')
    model_id_from_uri = model.uri[len("bq://"):].split('@')[0]
    model_project = model.metadata.get("project_id", model_id_from_uri.split('.')[0]); model_dataset = model.metadata.get("dataset_id", model_id_from_uri.split('.')[1]); model_name = model.metadata.get("model_id", model_id_from_uri.split('.')[-1]); model_location = model.metadata.get("location", location)
    bqml_model_id = f"{model_project}.{model_dataset}.{model_name}"
    client = bigquery.Client(project=project, location=location); job_config = bigquery.QueryJobConfig()
    inference_table_id = f"{inference_data.metadata['project_id']}.{inference_data.metadata['dataset_id']}.{inference_data.metadata['table_id']}"
    timestamp = time.strftime("%Y%m%d%H%M%S"); predictions_table_short_id = f"predictions_{model_name}_{timestamp}"; predictions_table_full_id = f"{project}.{dataset_id}.{predictions_table_short_id}"
    threshold_option = ""
    if prediction_threshold is not None:
        try: threshold_val = float(prediction_threshold); threshold_option = f", STRUCT({threshold_val} AS threshold)"
        except (ValueError, TypeError): logger.warning(f"Invalid prediction_threshold '{prediction_threshold}'. No threshold applied.")
    sql = f""" CREATE OR REPLACE TABLE `{predictions_table_full_id}` OPTIONS( description="Predictions by BQML model {bqml_model_id} on {timestamp}", expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 7 DAY) ) AS SELECT * FROM ML.PREDICT(MODEL `{bqml_model_id}`, TABLE `{inference_table_id}` {threshold_option} ); """
    try:
        query_job = client.query(sql, job_config=job_config, location=model_location); query_job.result()
        table = client.get_table(predictions_table_full_id); num_predictions = table.num_rows
        predictions.uri = f"bq://{predictions_table_full_id}"
        predictions.metadata = { 'table_id': table.table_id, 'dataset_id': table.dataset_id, 'project_id': table.project, 'location': table.location, 'num_rows': num_predictions, 'schema': [{'name': f.name, 'type': f.field_type} for f in table.schema], 'source_model_artifact_uri': model.uri, 'source_model_id': bqml_model_id, 'source_inference_table_id': inference_table_id }
        Outputs = collections_namedtuple('Outputs', ['predictions_table_full_id', 'predictions_count'])
        return Outputs(predictions_table_full_id, num_predictions)
    except Exception as e:
        logger.error(f"Error during BQML prediction {bqml_model_id}: {e}")
        if 'query_job' in locals() and hasattr(query_job, 'errors') and query_job.errors: logger.error(f"BQ Job Errors: {query_job.errors}")
        if 'query_job' in locals() and hasattr(query_job, 'error_result') and query_job.error_result: logger.error(f"BQ Error Result: {query_job.error_result}")
        raise

# Component 5: Extract Metadata ID from Model (REVISED AND SIMPLIFIED INPUT)
@component(base_image=base_image)
def extract_metadata_id_from_model(
    model_artifact_uri_str: str, # CHANGED: Take URI as a string
    gcp_project_id: str,
    gcp_region: str,
) -> NamedTuple('Outputs', [('metadata_table_id', str)]):
    from logging import Logger
    import logging
    from collections import namedtuple as collections_namedtuple
    from google.cloud import aiplatform
    from google.api_core import exceptions as google_exceptions


    def setup_component_logging(logger_name: str) -> Logger:
        logger = logging.getLogger(logger_name)
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', force=True)
        return logger

    # Use a VERY DISTINCT logger name to confirm this new code is running
    logger = setup_component_logging('kfp_DIRECT_META_FETCH_V3') # Updated logger name for clarity

    logger.info(f"Component 'kfp_DIRECT_META_FETCH_V3' started.")
    logger.info(f"Received Model Artifact URI string: {model_artifact_uri_str}")
    logger.info(f"Received GCP Project ID: {gcp_project_id}")
    logger.info(f"Received GCP Region: {gcp_region}")

    aiplatform.init(project=gcp_project_id, location=gcp_region)

    retrieved_artifact_metadata = None
    selected_artifact = None # Define selected_artifact here

    try:
        if not model_artifact_uri_str.startswith('bq://'):
            raise ValueError(f"Expected model_artifact_uri_str to start with 'bq://', but got: {model_artifact_uri_str}")

        # Construct the filter: uri must be an exact match.
        # The URI stored in ML Metadata for BQML models IS the bq:// path.
        # Filter also by schema_title="system.Model" as before.
        filter_str = f'uri="{model_artifact_uri_str}" AND schema_title="system.Model"'
        logger.info(f"Attempting to list artifacts with filter: {filter_str}")

        # The list method should use the initialized project/location.
        # We specify metadata_store_id explicitly for robustness.
        matching_artifacts = aiplatform.Artifact.list(
            filter=filter_str,
            metadata_store_id="default" # Assuming 'default' store
        )

        if not matching_artifacts:
            logger.error(f"No Vertex AI Artifact found with URI '{model_artifact_uri_str}' and schema 'system.Model'.")
            raise ValueError(f"No Vertex AI Artifact found matching URI '{model_artifact_uri_str}'.")

        # --- START: MODIFIED LOGIC TO SELECT THE CORRECT ARTIFACT ---
        METADATA_KEY = 'source_feature_metadata_table_id'
        valid_artifacts_with_key = []
        for art in matching_artifacts:
            # Check if metadata exists and contains the required key
            if art.metadata and METADATA_KEY in art.metadata and art.metadata.get(METADATA_KEY) is not None:
                 # Also add a basic format validation to the metadata value itself
                 metadata_value = art.metadata.get(METADATA_KEY)
                 if isinstance(metadata_value, str) and len(metadata_value.split('.')) == 3:
                      valid_artifacts_with_key.append(art)
                 else:
                      logger.warning(f"Found artifact {art.name} with key '{METADATA_KEY}' but invalid value format: '{metadata_value}'. Skipping this artifact.")
            else:
                logger.debug(f"Artifact {art.name} with URI '{model_artifact_uri_str}' does not contain the metadata key '{METADATA_KEY}' or metadata is empty. Skipping.")

        if not valid_artifacts_with_key:
            logger.error(f"No Vertex AI Artifact found with URI '{model_artifact_uri_str}' that contains the metadata key '{METADATA_KEY}' with a valid value.")
            logger.error(f"Found {len(matching_artifacts)} artifact(s) matching the URI, but none had the required key with a valid format. Example metadata from first match (if any): {matching_artifacts[0].metadata if matching_artifacts else 'N/A'}")
            raise ValueError(f"No artifact with URI '{model_artifact_uri_str}' found containing the key '{METADATA_KEY}' with a valid value.")

        # Sort the ones that *do* have the key by update_time to get the most recent one
        valid_artifacts_with_key.sort(key=lambda art: art.update_time, reverse=True)
        selected_artifact = valid_artifacts_with_key[0]
        # --- END: MODIFIED LOGIC ---

        logger.info(f"Selected Vertex AI Artifact (which contains '{METADATA_KEY}'): {selected_artifact.name} (updated: {selected_artifact.update_time})")
        logger.info(f"Full artifact details of selected: {selected_artifact.to_dict()}")
        retrieved_artifact_metadata = selected_artifact.metadata # This should now definitely have the key

    except google_exceptions.NotFound:
        logger.error(f"Vertex AI Artifacts with URI '{model_artifact_uri_str}' not found via client library.")
        raise
    except Exception as e:
        logger.error(f"Error during Vertex AI Artifact lookup or processing for URI '{model_artifact_uri_str}': {e}")
        # Log artifact details if selected_artifact was found before error
        if selected_artifact:
            try: logger.error(f"Details of selected artifact before error: {selected_artifact.to_dict()}")
            except Exception as log_err: logger.error(f"Error logging selected artifact details: {log_err}")
        raise

    # This check is now less likely to fail if valid_artifacts_with_key was not empty,
    # but kept for robustness against unexpected AI Platform SDK behavior.
    if not retrieved_artifact_metadata or METADATA_KEY not in retrieved_artifact_metadata:
         # This case should be caught by the 'if not valid_artifacts_with_key' block above now.
         # However, a final check doesn't hurt.
         logger.error(f"ASSERTION FAILED: Selected artifact {selected_artifact.name} did not contain '{METADATA_KEY}' after selection logic. Available metadata: {retrieved_artifact_metadata}")
         raise ValueError(f"Internal error: Selected artifact metadata missing key '{METADATA_KEY}'.")


    metadata_id = retrieved_artifact_metadata.get(METADATA_KEY)

    # Basic format validation on the value (already done in the loop, but double check)
    if not isinstance(metadata_id, str) or len(metadata_id.split('.')) != 3:
        logger.error(f"Value '{metadata_id}' for '{METADATA_KEY}' from artifact {selected_artifact.name} is invalid. Expected 'project.dataset.table'. Full metadata: {retrieved_artifact_metadata}")
        raise ValueError(f"Value '{metadata_id}' for '{METADATA_KEY}' is invalid. Expected 'project.dataset.table'.")

    logger.info(f"Successfully extracted metadata table ID: {metadata_id}")

    Outputs = collections_namedtuple('Outputs', ['metadata_table_id'])
    return Outputs(metadata_id)