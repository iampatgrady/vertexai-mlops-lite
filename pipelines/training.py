# /vai-basic/pipelines/training.py
import kfp.dsl as dsl
from kfp import compiler
import datetime
from pipelines import config # Get config loaded in __init__
from pipelines.components.bigquery_components import (
    create_feature_metadata,        # Uses updated V2/V3 component
    create_training_inference_data, # Uses updated V2/V3 component
    train_bqml_model,               # Uses updated V2/V3 component
)
# Import Model, Dataset artifacts if needed, placeholder constant is accessed via dsl object
from kfp.dsl import Model, Dataset # REMOVED PipelineIdPlaceholder from here

# Default model name based on config
_default_model_display_name = f"{config['bq_dataset_id']}_main_model"

# --- Pipeline Definition (Updated for V3 approach) ---
@dsl.pipeline(
    name="mini-mlops-dynamic-training-pipeline-v3",
    description="BQML Training Pipeline with Versioned Feature Metadata Linkage"
)
def training_pipeline(
    # Pipeline parameters load defaults from config
    gcp_project_id: str = config['gcp_project_id'],
    gcp_region: str = config['gcp_region'],
    bq_dataset_id: str = config['bq_dataset_id'],
    bq_dataset_location: str = config.get('bq_dataset_location', 'US'),
    model_display_name: str = _default_model_display_name,
    bqml_options: dict = config['model'].get('create_model_params', {}),
    analytics_project_id: str = config['analytics_ga4_project_id'],
    analytics_ga4_id: str = config['analytics_ga4_id'],
    analytics_ga4_stream_id: str = config['analytics_ga4_stream_id'],
    feature_analysis_lookback_days: int = config['feature_generation']['analysis_lookback_days'],
    top_n_string_values: int = config['feature_generation']['top_n_string_values'],
    sp_lookback_days: int = config['bq_sp_params']['lookback_days'],
    sp_lookahead_days: int = config['bq_sp_params']['lookahead_days']
):
    """
    Orchestrates BQML training with versioned feature metadata:
    1. Creates versioned feature metadata & persistent mapping tables using pipeline run ID.
    2. Creates training data using the specific metadata table ID generated in step 1.
    3. Trains BQML model, linking it to the specific metadata table ID used.
    """
    # --- Calculate Training Dates based on config (No change from original) ---
    training_config = config.get('training', {})
    start_date_str = None
    end_date_str = None
    date_fmt = '%Y-%m-%d'

    if 'data_date_start' in training_config and 'data_date_end' in training_config:
        start_date_str = training_config['data_date_start']
        end_date_str = training_config['data_date_end']
        try:
            datetime.datetime.strptime(start_date_str, date_fmt)
            datetime.datetime.strptime(end_date_str, date_fmt)
        except ValueError:
            raise ValueError("Invalid date format in training config (expected YYYY-MM-DD).")
        print(f"Using fixed training dates: {start_date_str} to {end_date_str}")
    elif 'data_date_start_days_ago' in training_config:
        days_ago = training_config['data_date_start_days_ago']
        if not isinstance(days_ago, int) or days_ago <= 0:
             raise ValueError("Invalid 'data_date_start_days_ago' in training config: must be positive integer.")
        end_date = datetime.date.today()
        start_date = end_date - datetime.timedelta(days=days_ago)
        start_date_str = start_date.strftime(date_fmt)
        end_date_str = end_date.strftime(date_fmt)
        print(f"Using relative training dates: {start_date_str} to {end_date_str} ({days_ago} days ago start)")
    else:
        raise ValueError("Missing valid date config in 'training' section. Provide data_date_start/end or data_date_start_days_ago.")
    # --- End Date Calculation ---

    # 1. Create Versioned Feature Metadata Table
    create_metadata_task = create_feature_metadata(
        project=gcp_project_id,
        location=bq_dataset_location,
        dataset_id=bq_dataset_id,
        analytics_project_id=analytics_project_id,
        analytics_ga4_id=analytics_ga4_id,
        analytics_ga4_stream_id=analytics_ga4_stream_id,
        analysis_lookback_days=feature_analysis_lookback_days,
        top_n_string_values=top_n_string_values,
        # --- Use the placeholder constant correctly ---
        pipeline_run_id=dsl.PIPELINE_JOB_ID_PLACEHOLDER
        # ---------------------------------------------
    ).set_display_name("Analyze Features & Create Metadata")

    # 2. Create Training Data using the specific Metadata Table ID generated above
    create_training_data_task = create_training_inference_data(
        project=gcp_project_id,
        location=bq_dataset_location,
        dataset_id=bq_dataset_id,
        feature_metadata_table_id=create_metadata_task.outputs["feature_metadata_table_id"],
        analytics_project_id=analytics_project_id,
        analytics_ga4_id=analytics_ga4_id,
        analytics_ga4_stream_id=analytics_ga4_stream_id,
        mode="TRAINING",
        data_date_start=start_date_str,
        data_date_end=end_date_str,
        lookback_days=sp_lookback_days,
        lookahead_days=sp_lookahead_days
    ).set_display_name("Create Training Data")
    create_training_data_task.after(create_metadata_task)

    # 3. Train BQML Model, passing the metadata table ID for linkage in the output artifact
    train_model_task = train_bqml_model(
        project=gcp_project_id,
        location=bq_dataset_location,
        dataset_id=bq_dataset_id,
        model_display_name=model_display_name,
        bqml_options=bqml_options,
        training_data=create_training_data_task.outputs["output_dataset"],
        feature_metadata_table_id=create_metadata_task.outputs["feature_metadata_table_id"]
    ).set_display_name(f"Train BQML Model ({_default_model_display_name})")
    train_model_task.after(create_training_data_task)


# --- Pipeline Compilation (if run directly) ---
if __name__ == '__main__':
    compiled_file_name = 'training_pipeline_v3_dynamic.yaml'
    compiler.Compiler().compile(
        pipeline_func=training_pipeline,
        package_path=compiled_file_name
    )
    print(f"V3 Training pipeline compiled to {compiled_file_name}")