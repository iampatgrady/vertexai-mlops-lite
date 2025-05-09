# /vai-basic/pipelines/prediction.py
import kfp.dsl as dsl
from kfp import compiler
import datetime
from typing import Optional

from pipelines import config
from pipelines.components.bigquery_components import (
    create_training_inference_data,
    predict_bqml_model,
    extract_metadata_id_from_model, # This will now be the REVISED component from above
)
from kfp.dsl import importer, Input, Output, Model, Dataset, PIPELINE_JOB_ID_PLACEHOLDER

_default_model_display_name = f"{config['bq_dataset_id']}_main_model"

@dsl.pipeline(
    name="mini-mlops-dynamic-prediction-pipeline-v3",
    description="BQML Prediction Pipeline using Model Artifact and Linked Metadata ID"
)
def prediction_pipeline(
    model_artifact_uri: str, # This is the bq://... string
    gcp_project_id: str = config['gcp_project_id'],
    gcp_region: str = config['gcp_region'],
    bq_dataset_id: str = config['bq_dataset_id'],
    bq_dataset_location: str = config.get('bq_dataset_location', 'US'),
    analytics_project_id: str = config['analytics_ga4_project_id'],
    analytics_ga4_id: str = config['analytics_ga4_id'],
    analytics_ga4_stream_id: str = config['analytics_ga4_stream_id'],
    sp_lookback_days: int = config['bq_sp_params']['lookback_days'],
    sp_lookahead_days: int = config['bq_sp_params']['lookahead_days'],
    prediction_threshold: Optional[float] = None
):
    prediction_config = config.get('prediction', {})
    date_fmt = '%Y-%m-%d'
    if 'data_date_start_days_ago' in prediction_config:
        days_ago = prediction_config['data_date_start_days_ago']
        if not isinstance(days_ago, int) or days_ago < 0:
             raise ValueError("Invalid 'data_date_start_days_ago' in prediction config.")
        end_date = datetime.date.today()
        start_date = end_date - datetime.timedelta(days=days_ago)
        start_date_str = start_date.strftime(date_fmt)
        end_date_str = end_date.strftime(date_fmt)
    else:
        raise ValueError("Missing 'data_date_start_days_ago' in 'prediction' config.")

    # The importer task is still useful for passing an actual Model artifact to downstream
    # components that expect Input[Model] (like the predict_bqml_model component).
    # It also helps with lineage visualization in the UI.
    import_model_task = dsl.importer(
         artifact_uri=model_artifact_uri, # Use the input string URI
         artifact_class=Model,
         reimport=False, # Keep False
         metadata={'pipeline_run_id_for_importer': dsl.PIPELINE_JOB_ID_PLACEHOLDER} # Cache buster for importer
    ).set_display_name("Import BQML Model Artifact (for lineage & downstream)")

    # Step 2: Extract the linked Feature Metadata Table ID (using REVISED component)
    # THIS COMPONENT NOW TAKES THE URI STRING DIRECTLY.
    extract_metadata_id_task = extract_metadata_id_from_model(
        model_artifact_uri_str=model_artifact_uri, # CHANGED: Pass the pipeline input string URI
        gcp_project_id=gcp_project_id,
        gcp_region=gcp_region
    ).set_display_name("Extract Metadata ID (Direct Fetch V2)")
    # No formal dependency on import_model_task needed for this component's logic,
    # but good to keep tasks ordered if desired.
    # extract_metadata_id_task.after(import_model_task) # Optional: can remove if not strictly needed

    # Step 3: Create Inference Data
    create_inference_data_task = create_training_inference_data(
        project=gcp_project_id,
        location=bq_dataset_location,
        dataset_id=bq_dataset_id,
        feature_metadata_table_id=extract_metadata_id_task.outputs["metadata_table_id"],
        analytics_project_id=analytics_project_id,
        analytics_ga4_id=analytics_ga4_id,
        analytics_ga4_stream_id=analytics_ga4_stream_id,
        mode="INFERENCE",
        data_date_start=start_date_str,
        data_date_end=end_date_str,
        lookback_days=sp_lookback_days,
        lookahead_days=sp_lookahead_days
    ).set_display_name("Create Inference Data from Linked Metadata")
    create_inference_data_task.after(extract_metadata_id_task) # Depends on successful extraction

    # Step 4: Generate Predictions
    # This component still needs an Input[Model]
    predict_task = predict_bqml_model(
        project=gcp_project_id,
        location=bq_dataset_location,
        dataset_id=bq_dataset_id,
        model=import_model_task.outputs['artifact'], # Use the output of the importer task here
        inference_data=create_inference_data_task.outputs["output_dataset"],
        prediction_threshold=prediction_threshold
    ).set_display_name(f"Predict using Imported BQML Model")
    predict_task.after(create_inference_data_task)

if __name__ == '__main__':
    compiled_file_name = 'prediction_pipeline_v3_dynamic.yaml'
    compiler.Compiler().compile(
        pipeline_func=prediction_pipeline,
        package_path=compiled_file_name
    )
    print(f"V3 Prediction pipeline compiled to {compiled_file_name}")