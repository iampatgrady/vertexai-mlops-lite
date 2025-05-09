# /vai-basic/terraform/helpers.py

import argparse
import os
import sys
import yaml
import logging
import time
import google.auth
import google.auth.transport.requests
from google.auth.exceptions import DefaultCredentialsError
from google.api_core.exceptions import NotFound, GoogleAPICallError, PermissionDenied
from jinja2 import Environment, FileSystemLoader
import traceback
import copy
import unicodedata
from datetime import datetime # Import datetime for parsing timestamps
import dateutil.parser # Import dateutil parser for robust timestamp parsing
import json # For pretty printing

# Add project root directory to sys.path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
     sys.path.insert(0, project_root)

try:
    from google.cloud import aiplatform
    from google.cloud.aiplatform import pipeline_jobs, PipelineJobSchedule
    import kfp
    import kfp.dsl
    from kfp import compiler
    from kfp.registry import RegistryClient
except ImportError as e:
    print(f"\n{'*' * 20} PYTHON IMPORT ERROR {'*' * 20}")
    print(f"FATAL: Failed to import required Python packages: {e}")
    sys.exit(1)


# --- Set up logging ---
log_level = logging.INFO
logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s', stream=sys.stderr)
logger = logging.getLogger(__name__)

# --- Function definitions ---

def retrieve_config(config_path_arg=None):
    """Loads and performs basic validation on the config YAML."""
    if config_path_arg:
         config_path = config_path_arg
         if not os.path.isabs(config_path):
              config_path = os.path.abspath(config_path)
    else:
        config_path_env = os.environ.get("TF_VAR_config_path", "config.yaml")
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(script_dir, "..", config_path_env)

    logger.info(f"Attempting to load configuration from: {config_path}")
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ValueError(f"Error parsing YAML file {config_path}: {e}")
    except Exception as e:
        raise IOError(f"Could not read config file {config_path}: {e}")

    if not config:
        raise ValueError(f"Config file {config_path} is empty or invalid.")
    required_keys = ['gcp_project_id', 'gcp_region', 'bq_dataset_id', 'model']
    for key in required_keys:
        if key not in config:
             raise ValueError(f"Missing required key '{key}' in {config_path}")

    if config.get('model', {}).get('type') != 'BQML':
        raise ValueError("Minimal version only supports model.type = BQML")

    logger.info("Configuration loaded successfully.")
    return config

def get_gcp_credentials():
     """Gets default GCP credentials and refreshes them."""
     try:
          credentials, project_id = google.auth.default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
          if not credentials:
               raise DefaultCredentialsError("Could not get default GCP credentials (returned None).")
          auth_req = google.auth.transport.requests.Request()
          credentials.refresh(auth_req)
          if not credentials.valid:
                raise DefaultCredentialsError("Refreshed credentials are not valid.")
          logger.info(f"Successfully obtained/refreshed GCP credentials for project: {project_id or 'Default'}")
          return credentials
     except DefaultCredentialsError as e:
          logger.error(f"Could not get default GCP credentials. Ensure authenticated. Error: {e}")
          raise
     except Exception as e:
         logger.error(f"An unexpected error during credential retrieval: {e}")
         raise

def compile_pipeline_func(pipeline_func: callable, template_path: str, pipeline_name: str):
    """Compiles a KFP pipeline function to a YAML file."""
    logger.info(f"Compiling pipeline '{pipeline_name}' to {template_path}...")
    try:
        if not os.path.isabs(template_path):
            template_path = os.path.join(project_root, template_path)

        output_dir = os.path.dirname(template_path)
        if output_dir: os.makedirs(output_dir, exist_ok=True)

        effective_pipeline_name = getattr(pipeline_func, '_pipeline_name', pipeline_name)

        compiler.Compiler().compile(
            pipeline_func=pipeline_func,
            package_path=template_path,
            pipeline_name=effective_pipeline_name
        )
        logger.info(f"Pipeline '{effective_pipeline_name}' compiled successfully to {template_path}.")
    except Exception as e:
        logger.error(f"Error compiling pipeline {pipeline_name}: {e}")
        logger.error(traceback.format_exc())
        raise

def get_artifact_registry_client(project_id: str, region: str, repo_name: str):
    """Initializes the KFP Registry Client."""
    host = f"https://{region}-kfp.pkg.dev/{project_id}/{repo_name}"
    logger.info(f"Attempting to connect to Artifact Registry: {host}")
    try:
        _ = get_gcp_credentials() # Ensure credentials are fresh for the client
        client = RegistryClient(host=host)
        logger.info(f"Successfully initialized RegistryClient for Artifact Registry: {host}")
        return client
    except Exception as e:
        logger.error(f"Failed to initialize/connect RegistryClient for {host}: {e}")
        logger.error(traceback.format_exc())
        raise

def deploy_pipeline_to_registry(config: dict, ptype: str, repo_name: str, pipeline_name: str, compiled_template_path: str):
    """Uploads a compiled pipeline YAML to Google Artifact Registry, ensuring 'latest' tag."""
    if not os.path.isabs(compiled_template_path):
         compiled_template_path = os.path.join(project_root, compiled_template_path)
    if not os.path.exists(compiled_template_path):
         raise FileNotFoundError(f"Compiled pipeline file not found: {compiled_template_path}.")

    client = get_artifact_registry_client(config['gcp_project_id'], config['gcp_region'], repo_name)
    
    # Always include 'latest' and a timestamped version tag
    current_time_tag = f"v{int(time.time())}"
    tags_to_apply = ["latest", current_time_tag]
    
    logger.info(f"Uploading {compiled_template_path} to Artifact Registry (package: '{pipeline_name}', tags: {tags_to_apply})...")
    
    uploaded_package_name = None
    uploaded_version_id = None # This is the version SHA
    
    try:
        # The upload_pipeline method from kfp.registry.RegistryClient returns (package_name, version_id)
        # where version_id is the sha256 digest of the package.
        response = client.upload_pipeline(
             file_name=compiled_template_path,
             tags=tags_to_apply # Pass the tags here
         )
        logger.debug(f"upload_pipeline response: {response}, type: {type(response)}")

        if isinstance(response, tuple) and len(response) >= 2:
             uploaded_package_name, uploaded_version_id = response[0], response[1]
             logger.info(f"Pipeline uploaded: Package='{uploaded_package_name}', VersionID(SHA)='{uploaded_version_id}', Tags='{tags_to_apply}'.")
             if uploaded_package_name != pipeline_name:
                 logger.warning(f"Registry returned package name '{uploaded_package_name}' which differs from expected '{pipeline_name}'. This is usually fine if the YAML's internal pipeline name was different.")
        else:
            # This block should ideally not be hit if kfp.registry.RegistryClient behaves as documented.
            logger.error(f"Unexpected response format from upload_pipeline: {response}. Expected a tuple.")
            # Fallback to try and get version info if possible (less reliable)
            if hasattr(response, 'version_id'): uploaded_version_id = response.version_id
            elif hasattr(response, 'name') and isinstance(response.name, str) and '/' in response.name: uploaded_version_id = response.name.split('/')[-1]
            raise ValueError(f"Failed to get clear package_name and version_id from upload_pipeline response: {response}")

        # The 'latest' tag should have been applied by upload_pipeline itself.
        # No need to call create_tag or update_tag separately if upload_pipeline handles it.
        # Double-check by trying to get the tag (optional, for verbosity or debugging)
        try:
            tag_info = client.get_tag(package_name=uploaded_package_name, tag='latest')
            logger.info(f"Confirmed 'latest' tag points to version: {tag_info.get('version')}")
            if tag_info.get('version') != uploaded_version_id:
                logger.warning(f"'latest' tag points to {tag_info.get('version')}, but uploaded version was {uploaded_version_id}. This might happen due to eventual consistency or if another process updated the tag.")
        except Exception as tag_e:
            logger.warning(f"Could not confirm 'latest' tag details immediately after upload: {tag_e}")

        time.sleep(5) # Allow some time for registry to become consistent
        return f"Package='{uploaded_package_name}', Version(SHA)='{uploaded_version_id}', Tags Applied='{tags_to_apply}'"

    except Exception as e:
        logger.error(f"Error uploading pipeline from {compiled_template_path}: {e}")
        logger.error(traceback.format_exc())
        raise

def get_latest_pipeline_version_uri(config: dict, repo_name: str, package_name: str) -> str:
    """Retrieves the full URI of the latest version for a pipeline package in Artifact Registry."""
    client = get_artifact_registry_client(config['gcp_project_id'], config['gcp_region'], repo_name)
    project_id = config['gcp_project_id']
    region = config['gcp_region']
    logger.info(f"Fetching latest version URI for package '{package_name}' in repo '{repo_name}'...")
    max_retries = 3
    retry_delay = 15 # Increased delay slightly

    for attempt in range(max_retries):
        try:
            versions_data = client.list_versions(package_name=package_name) # List of dicts
            logger.debug(f"Attempt {attempt + 1}: Raw versions_data for '{package_name}':\n{json.dumps(versions_data, indent=2)}")

            if not versions_data:
                if attempt < max_retries - 1:
                    logger.warning(f"No versions found yet for '{package_name}', retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
                    continue
                else:
                    raise ValueError(f"No versions found for package '{package_name}' after {max_retries} attempts.")

            latest_version_dict = None
            # 1. Prioritize finding the version explicitly tagged 'latest'
            for v_dict in versions_data:
                tags = v_dict.get('tags', [])
                if isinstance(tags, str): tags = [tags]
                if 'latest' in tags:
                     latest_version_dict = v_dict
                     logger.info(f"Found version tagged 'latest': Name='{v_dict.get('name')}', VersionID='{v_dict.get('versionId', v_dict.get('version'))}', UpdateTime='{v_dict.get('updateTime')}'")
                     break
            
            # 2. Fallback: If 'latest' tag not found, sort by 'updateTime' or 'createTime'
            if not latest_version_dict:
                logger.warning(f"No 'latest' tag found for '{package_name}'. Sorting by timestamp to find most recent.")
                
                def get_sortable_time(v_dict):
                    # Google API often uses 'updateTime' or 'createTime'
                    # The kfp.RegistryClient might normalize these. Let's check common patterns.
                    # The actual key from client.list_versions() seems to be 'update_time' (lowercase with underscore)
                    # based on previous error. However, API responses often use camelCase 'updateTime'.
                    # Let's be flexible.
                    timestamp_str = v_dict.get('updateTime') or v_dict.get('update_time') or \
                                    v_dict.get('createTime') or v_dict.get('create_time')
                    if timestamp_str:
                        try:
                            return dateutil.parser.isoparse(timestamp_str)
                        except (ValueError, TypeError) as parse_err:
                            logger.warning(f"Could not parse timestamp '{timestamp_str}' for version '{v_dict.get('name')}': {parse_err}")
                            return datetime.min.replace(tzinfo=dateutil.tz.UTC) # Fallback for unparseable
                    logger.warning(f"No recognizable timestamp found for version '{v_dict.get('name')}'. Version data: {v_dict}")
                    return datetime.min.replace(tzinfo=dateutil.tz.UTC) # Default for sorting if no time found

                try:
                    # Sort by the parsed timestamp, most recent first
                    versions_data.sort(key=get_sortable_time, reverse=True)
                    if versions_data: # Check if list is not empty after potential filtering by get_sortable_time
                        latest_version_dict = versions_data[0]
                        logger.info(f"Using most recent version by timestamp: Name='{latest_version_dict.get('name')}', VersionID='{latest_version_dict.get('versionId', latest_version_dict.get('version'))}', UpdateTime='{latest_version_dict.get('updateTime', latest_version_dict.get('update_time'))}'")
                    else:
                        # This should not happen if the initial versions_data was not empty.
                        raise ValueError(f"Version list became empty after attempting to sort by time for package '{package_name}'.")
                except Exception as sort_e:
                    logger.error(f"Failed to sort versions by time: {sort_e}. Traceback: {traceback.format_exc()}")
                    # Critical fallback: If sorting fails catastrophically, we might have to error out or use a very naive approach.
                    # For now, let the original "if not latest_version_dict" after this block handle it or raise.
                    # Re-raising to ensure the problem is visible.
                    raise ValueError(f"Critical error during version sorting for package '{package_name}'.") from sort_e
            
            if not latest_version_dict: # Should be populated by now
                 raise ValueError(f"Could not determine the latest version for package '{package_name}' from available data.")

            # Extract version_id (the SHA) and package name from the resource name string.
            # The 'name' field is typically projects/.../packages/PACKAGE_NAME/versions/VERSION_ID_SHA
            version_resource_name = latest_version_dict.get('name')
            if not version_resource_name or not isinstance(version_resource_name, str) or '/' not in version_resource_name:
                raise ValueError(f"Could not extract valid resource name (projects/.../versions/SHA) from latest version dict: {latest_version_dict}")

            parts = version_resource_name.split('/')
            if len(parts) != 10:
                 logger.error(f"Resource name parts count is {len(parts)}, expected 10. Name: {version_resource_name}")
                 raise ValueError(f"Unexpected resource name format: {version_resource_name}")

            retrieved_package_name_from_uri = parts[7]
            retrieved_version_id_sha = parts[9] # This is the SHA (e.g., sha256:abcdef...)
            
            # The URI needs to be in the format: https://{region}-kfp.pkg.dev/{project_id}/{repo_name}/{package_name}/{version_id_sha}
            uri = f"https://{region}-kfp.pkg.dev/{project_id}/{repo_name}/{retrieved_package_name_from_uri}/{retrieved_version_id_sha}"

            if retrieved_package_name_from_uri != package_name:
                logger.warning(f"Package name from registry URI ('{retrieved_package_name_from_uri}') differs from initially queried package_name ('{package_name}'). Using the name from the URI.")

            logger.info(f"Determined latest version URI for scheduling: {uri}")
            return uri

        except (GoogleAPICallError, ValueError, PermissionDenied, TypeError, AttributeError) as e: # Removed NotImplementedError for now
            logger.error(f"Error fetching latest version URI for '{package_name}' on attempt {attempt + 1}: {e}")
            logger.error(traceback.format_exc())
            if attempt < max_retries - 1:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                raise ValueError(f"Could not get latest version URI for package '{package_name}' after multiple retries.")
        except Exception as e_unhandled: # Catch any other unexpected errors
            logger.error(f"An UNEXPECTED error occurred while fetching latest version URI for '{package_name}' on attempt {attempt+1}: {e_unhandled}")
            logger.error(traceback.format_exc())
            if attempt < max_retries - 1:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                raise ValueError(f"Could not get latest version URI for package '{package_name}' due to an unhandled exception after multiple retries.")

# --- Pipeline Scheduling (Updated for v1.90.0 API) ---
def schedule_pipeline(
    config: dict,
    ptype: str, # 'training' or 'prediction'
    repo_name: str,
    pipeline_name: str, # Name for Schedule display AND package name in registry
    pipeline_root: str
) -> None:
    """Schedules a Vertex AI Pipeline from an Artifact Registry template."""
    project = config['gcp_project_id']
    region = config['gcp_region']

    try:
        pipeline_template_uri = get_latest_pipeline_version_uri(config, repo_name, pipeline_name)
        logger.info(f"Using latest pipeline template URI for scheduling: {pipeline_template_uri}")
    except Exception as e:
         logger.error(f"Failed to get latest pipeline version URI for '{pipeline_name}'. Cannot schedule. Error: {e}")
         raise

    logger.info(f"Attempting to schedule pipeline '{pipeline_name}' from {pipeline_template_uri}")
    try:
        aiplatform.init(project=project, location=region)
        logger.info(f"AI Platform client initialized for scheduling.")
    except Exception as e:
        logger.error(f"Failed to initialize AI Platform client: {e}"); raise

    if ptype not in config or 'cron' not in config.get(ptype, {}):
         raise ValueError(f"Cron schedule not found for '{ptype}' in config.")
    cron = config[ptype]['cron']
    service_account = config.get('gcp_service_account', None)

    if not service_account:
         logger.warning("gcp_service_account not specified. Attempting to derive Compute Engine default SA.")
         try:
             credentials = get_gcp_credentials()
             # Fallback: Assume the default SA pattern if helper fails
             try:
                 # This specific helper might not exist in all versions.
                 # For project number, google.auth should provide it if credentials are for a project
                 if hasattr(credentials, 'project_id') and credentials.project_id: # Should be true for default creds
                    # To get project number, we'd typically call cloudresourcemanager.projects.get
                    # This is an extra API call. Vertex might be able to use the default compute SA
                    # if we just pass None for service_account and the compute SA has perms.
                    # Let's try a simpler approach: If gcp_service_account is blank, pass None to Vertex.
                    # Vertex Pipelines will then try to use the default Compute Engine SA for the region if not specified.
                    # The permissions for this SA should already be granted by Terraform if we use it implicitly.
                    # project_number = aiplatform.helpers.get_project_number(project, credentials=credentials) # This helper is deprecated/gone
                    # For now, if not specified, we'll pass None and rely on Vertex defaults / existing CE SA permissions.
                    logger.info("No explicit gcp_service_account. Vertex will use its default (likely Compute Engine SA if properly permissioned).")
                    service_account = None # Explicitly set to None if not in config
                 else:
                    logger.error("Could not get project_id from credentials to attempt SA derivation.")
                    service_account = None
             except Exception as sa_derive_e:
                 logger.error(f"Error during SA derivation logic: {sa_derive_e}. Proceeding with service_account as None.")
                 service_account = None
         except Exception as sa_e:
             logger.error(f"Could not determine default service account: {sa_e}.")
             service_account = None

    parameter_values = {}
    common_params = {
        "gcp_project_id": project, "gcp_region": region, "bq_dataset_id": config['bq_dataset_id'],
        "bq_dataset_location": config.get('bq_dataset_location', 'US'),
        "analytics_project_id": config.get('analytics_ga4_project_id',''), "analytics_ga4_id": config.get('analytics_ga4_id',''),
        "analytics_ga4_stream_id": config.get('analytics_ga4_stream_id',''),
        "sp_lookback_days": config.get('bq_sp_params',{}).get('lookback_days', 28),
        "sp_lookahead_days": config.get('bq_sp_params',{}).get('lookahead_days', 7),
    }
    if ptype == 'training':
        parameter_values = {
            **common_params, "model_display_name": f"{config['bq_dataset_id']}_main_model",
            "bqml_options": config.get('model',{}).get('create_model_params', {}), # This is a dict
            "feature_analysis_lookback_days": config.get('feature_generation',{}).get('analysis_lookback_days', 14),
            "top_n_string_values": config.get('feature_generation',{}).get('top_n_string_values', 10), }
    elif ptype == 'prediction':
        model_name_for_prediction = f"{config['bq_dataset_id']}_main_model"
        # Construct the BQ model URI. It should NOT be an artifact URI but a BQ path.
        # The pipeline's Importer task will convert this to an artifact.
        # However, the prediction_pipeline takes model_artifact_uri as a string.
        # For scheduled runs, this model_artifact_uri needs to point to a *specific, existing* model version.
        # The current setup implies it always uses the "main_model". This is a point for future improvement
        # (e.g., getting the latest successful training run's model URI).
        # For now, we use the configured main model name.
        model_artifact_uri_to_use = f"bq://{project}.{config['bq_dataset_id']}.{model_name_for_prediction}"
        logger.info(f"Constructed Model URI for prediction schedule parameter: {model_artifact_uri_to_use}")
        parameter_values = {
            **common_params, "model_artifact_uri": model_artifact_uri_to_use,
            "prediction_threshold": config.get('prediction', {}).get('threshold', None)
        }
        # Ensure threshold is float or None
        if parameter_values["prediction_threshold"] is not None:
            try:
                parameter_values["prediction_threshold"] = float(parameter_values["prediction_threshold"])
            except (ValueError, TypeError):
                logger.warning(f"Prediction threshold '{parameter_values['prediction_threshold']}' is not a valid float. Setting to None.")
                parameter_values["prediction_threshold"] = None


    logger.info(f"Prepared parameter_values for {ptype} schedule: {json.dumps(parameter_values, indent=2)}")

    schedule_display_name = pipeline_name

    try:
        logger.info(f"Checking for existing schedules named '{schedule_display_name}'...")
        all_schedules = list(aiplatform.PipelineJobSchedule.list(filter=f'display_name="{schedule_display_name}"')) # Filter server-side
        
        if all_schedules: logger.info(f"Found {len(all_schedules)} existing schedule(s) named '{schedule_display_name}'. Deleting.")
        else: logger.info(f"No existing schedule found with display name '{schedule_display_name}'.")

        for schedule_resource in all_schedules:
             try:
                 logger.info(f"Deleting schedule: {schedule_resource.resource_name} (Display Name: {schedule_resource.display_name})")
                 schedule_resource.delete()
                 logger.info(f"Delete call issued for {schedule_resource.resource_name}. Waiting for confirmation...")
                 # Wait for deletion. The SDK's delete() might be synchronous or return an LRO.
                 # For simplicity, a short sleep. For robustness, poll the LRO or re-list.
                 time.sleep(15) # Increased wait after delete
             except NotFound:
                 logger.warning(f"Schedule {schedule_resource.resource_name} was not found during delete attempt (likely already deleted or race condition).")
             except Exception as del_err:
                 logger.error(f"Failed to delete schedule {schedule_resource.resource_name}: {del_err}. Continuing...")


        logger.info(f"Creating new pipeline schedule: '{schedule_display_name}'.")
        pipeline_job_obj = pipeline_jobs.PipelineJob(
            display_name=f"{schedule_display_name}_scheduled_run", # Name for the job instances run by schedule
            template_path=pipeline_template_uri,
            pipeline_root=pipeline_root,
            enable_caching=False, # Typically False for scheduled production runs
            project=project,
            location=region,
            parameter_values=parameter_values,
            labels={"app": "mini-mlops", "pipeline_type": ptype, "terraform_managed": "true"}
        )
        logger.debug("PipelineJob object for schedule defined.")

        schedule_obj_instance = PipelineJobSchedule(
            pipeline_job=pipeline_job_obj,
            display_name=schedule_display_name,
            project=project,
            location=region
        )
        logger.debug("PipelineJobSchedule object instantiated.")

        schedule_obj_instance.create(
            cron=cron,
            max_concurrent_run_count=1, # Sensible default
            service_account=service_account # Pass None if not specified to use Vertex default
            # start_time / end_time can be added if needed
        )
        logger.info(f"Schedule '{schedule_display_name}' creation submitted. Resource name: {schedule_obj_instance.resource_name}")

    except (GoogleAPICallError, PermissionDenied, TypeError, ValueError) as e:
        logger.error(f"Failed to create/manage schedule '{schedule_display_name}': {e}")
        if isinstance(e, PermissionDenied): logger.error(f"Hint: Check permissions for SA '{service_account or 'Vertex AI Default SA'}'.")
        elif "cron" in str(e).lower(): logger.error(f"Hint: Verify cron expression: '{cron}'.")
        elif "parameter_values" in str(e).lower(): logger.error(f"Hint: Check parameter_values: {parameter_values}")
        logger.error(traceback.format_exc()); raise
    except Exception as e:
         logger.error(f"Unexpected error during schedule creation/management for '{schedule_display_name}': {e}")
         logger.error(traceback.format_exc()); raise

def delete_schedules(project_id: str, region: str, pipeline_name_to_delete: str) -> None:
    """Deletes all Vertex AI Pipeline Schedules with a specific display name."""
    logger.setLevel(logging.DEBUG); logger.debug(f"Set log level DEBUG for delete_schedules: '{pipeline_name_to_delete}'")
    logger.info(f"Attempting to delete schedules with display name: '{pipeline_name_to_delete}' in {project_id}/{region}")
    try:
        aiplatform.init(project=project_id, location=region)
        logger.info(f"AI Platform client initialized for deletion.")
    except Exception as e:
        logger.error(f"Failed to initialize AI Platform client for deletion: {e}")
        return # Cannot proceed

    deleted_count = 0
    schedules_found_matching = []
    try:
        logger.info(f"Listing schedules matching display name '{pipeline_name_to_delete}' (server-side filter)...")
        # Use server-side filtering for efficiency
        schedules_to_delete = list(aiplatform.PipelineJobSchedule.list(filter=f'display_name="{pipeline_name_to_delete}"'))
        
        if not schedules_to_delete:
            logger.warning(f"No schedules found with display name '{pipeline_name_to_delete}'. Nothing to delete.")
            return

        logger.info(f"Found {len(schedules_to_delete)} schedule(s) matching display name for deletion.")
        schedules_found_matching = schedules_to_delete # For final log

        for schedule_resource in schedules_to_delete:
            schedule_name_uri = schedule_resource.resource_name
            display_name_from_obj = schedule_resource.display_name
            try:
                logger.info(f"Deleting schedule: {schedule_name_uri} (Display Name: '{display_name_from_obj}')")
                schedule_resource.delete() # This should be synchronous or return LRO handled by SDK
                # Add a small delay to allow API to process, though SDK delete might handle this.
                time.sleep(5) 
                deleted_count += 1
                logger.info(f"Deletion call successful for {schedule_name_uri}.")
            except NotFound:
                logger.warning(f"Schedule {schedule_name_uri} not found during delete (already deleted or race condition).")
            except (GoogleAPICallError, PermissionDenied) as e:
                logger.error(f"API error deleting schedule {schedule_name_uri}: {e}")
            except Exception as e_unhandled:
                logger.error(f"Unexpected error deleting schedule {schedule_name_uri}: {e_unhandled}")
    
    except (GoogleAPICallError, PermissionDenied) as e:
        logger.error(f"API error listing schedules: {e}")
    except Exception as e_unhandled:
        logger.error(f"Unexpected error during schedule listing for deletion: {e_unhandled}")
    finally:
        logger.info(f"Finished delete operation for schedules named '{pipeline_name_to_delete}'. Found: {len(schedules_found_matching)}, Delete calls issued for: {deleted_count}.")
        logger.setLevel(log_level) # Reset log level to original

def main(args):
    """Main driver script called by Terraform."""
    if args.erase:
        logger.setLevel(logging.DEBUG); logger.debug("Log level DEBUG for ERASE operation.")
        ptype_for_log = "unknown_type"
        if args.training: ptype_for_log = "training"
        elif args.prediction: ptype_for_log = "prediction"

        project_id = os.environ.get("PIPELINE_PROJECT_ID")
        region = os.environ.get("PIPELINE_REGION")
        pipeline_name_for_erase = os.environ.get("PIPELINE_NAME")

        logger.debug(f"ERASE - Read from env: PROJECT_ID='{project_id}', REGION='{region}', PIPELINE_NAME='{pipeline_name_for_erase}'")

        if not (project_id and region and pipeline_name_for_erase):
             logger.error("Erase failed: Missing required environment variables (PIPELINE_PROJECT_ID, PIPELINE_REGION, or PIPELINE_NAME)."); sys.exit(1)
        
        try:
             logger.info(f"\n--- Processing ERASE for {ptype_for_log.capitalize()} pipeline schedule named '{pipeline_name_for_erase}' ---")
             delete_schedules(project_id, region, pipeline_name_for_erase)
             logger.info(f"Erase operation completed for schedule named '{pipeline_name_for_erase}'.")
             sys.exit(0)
        except Exception as e:
            logger.error(f"Erase operation failed for schedule named '{pipeline_name_for_erase}': {e}")
            traceback.print_exc()
            sys.exit(1)

    config = None
    try:
        config = retrieve_config(args.config_path)
        project_id = config['gcp_project_id']
        region = config['gcp_region']
    except Exception as e:
        logger.error(f"Initialization error: {e}")
        sys.exit(1)

    ptype = None
    if args.training: ptype = 'training'
    elif args.prediction: ptype = 'prediction'
    if not ptype:
        logger.error("Pipeline type (--training or --prediction) not specified.")
        sys.exit(1)

    pipeline_required_config = config.get(ptype)
    if not pipeline_required_config:
        logger.warning(f"Configuration section for '{ptype}' not found in config.yaml. Skipping {ptype} pipeline operations.")
        return # Exit this specific invocation of main() if config for ptype is missing

    # --- Define names based on config and ptype ---
    repo_base_name = config['bq_dataset_id'].replace('_', '-')
    pipeline_repo_name = f"{repo_base_name}-pipelines" # Artifact Registry for KFP templates
    pipeline_root = f"gs://{project_id}-{repo_base_name}-pipelines" # GCS root for Vertex runs
    
    # This 'pipeline_name' is used for:
    # 1. The KFP package name in Artifact Registry.
    # 2. The display name of the Vertex AI PipelineJobSchedule.
    pipeline_name = f"mini-mlops-{ptype}-{repo_base_name}" 
    
    # The compiled YAML filename (local temporary file)
    compiled_template_filename = f"{ptype}_pipeline_dynamic.yaml" # e.g., training_pipeline_dynamic.yaml
    compiled_template_path = os.path.join(project_root, compiled_template_filename)
    # ---

    pipeline_func = None
    actions_requiring_func_load = args.compile or (args.deploy and not os.path.exists(compiled_template_path))
    
    if actions_requiring_func_load:
        logger.info(f"Loading pipeline function for '{ptype}' for compile/deploy action.")
        if ptype == 'training':
            try:
                from pipelines.training import training_pipeline
                pipeline_func = training_pipeline
            except ImportError as e:
                logger.error(f"ImportError loading training_pipeline: {e}")
                sys.exit(1)
        elif ptype == 'prediction':
            try:
                from pipelines.prediction import prediction_pipeline
                pipeline_func = prediction_pipeline
            except ImportError as e:
                logger.error(f"ImportError loading prediction_pipeline: {e}")
                sys.exit(1)
        if not pipeline_func:
             logger.error(f"Pipeline function for '{ptype}' could not be loaded.")
             sys.exit(1)

    try:
        if args.compile:
            if not pipeline_func:
                raise ValueError(f"Pipeline function for '{ptype}' is required for compile action but was not loaded.")
            # The `pipeline_name` passed here is used as `pipeline_name` in KFP compiler.
            # It's also the name we'll use for the package in Artifact Registry.
            compile_pipeline_func(pipeline_func, compiled_template_path, pipeline_name)
        
        if args.deploy:
            if not os.path.exists(compiled_template_path):
                if actions_requiring_func_load and pipeline_func: # Check if it should have been compiled
                     logger.info(f"Compiled template {compiled_template_path} not found. Compiling before deploy...")
                     compile_pipeline_func(pipeline_func, compiled_template_path, pipeline_name)
                else: # If not supposed to compile and file doesn't exist, it's an error
                     raise FileNotFoundError(f"Cannot deploy: Compiled pipeline file {compiled_template_path} not found and compile action not specified or failed.")
            # The `pipeline_name` here is the target package name in Artifact Registry.
            deploy_info = deploy_pipeline_to_registry(config, ptype, pipeline_repo_name, pipeline_name, compiled_template_path)
            logger.info(f"Deployment initiated for '{pipeline_name}'. Registry Info: {deploy_info}")
        
        if args.schedule:
             if 'cron' not in pipeline_required_config:
                 raise ValueError(f"Cannot schedule: 'cron' expression missing in '{ptype}' section of config.yaml.")
             # The `pipeline_name` here is:
             # 1. The package name in Artifact Registry from which to fetch the template URI.
             # 2. The display name for the PipelineJobSchedule in Vertex AI.
             schedule_pipeline(config, ptype, pipeline_repo_name, pipeline_name, pipeline_root)
        
        logger.info(f"Successfully completed requested actions for {ptype} pipeline ('{pipeline_name}').")

    except Exception as e:
        logger.error(f"Pipeline operation failed for '{ptype}' pipeline ('{pipeline_name}'): {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mini VAI-MLOps Terraform Helper Script")
    parser.add_argument("-f", "--config_path", help="Path to the config.yaml file.", default=None)
    parser.add_argument("-t", "--training", action="store_true", help="Target the training pipeline.")
    parser.add_argument("-p", "--prediction", action="store_true", help="Target the prediction pipeline.")
    parser.add_argument("-c", "--compile", action="store_true", help="Compile the targeted pipeline(s).")
    parser.add_argument("-d", "--deploy", action="store_true", help="Deploy compiled YAML(s) to Artifact Registry.")
    parser.add_argument("-s", "--schedule", action="store_true", help="Schedule pipeline(s) on Vertex AI.")
    parser.add_argument("-e", "--erase", action="store_true", help="Delete schedule(s) from Vertex AI (for destroy).")
    cli_args = parser.parse_args()

    if cli_args.erase:
        if not (cli_args.training or cli_args.prediction):
            parser.error("Erase action requires a target pipeline type specified with --training or --prediction.")
        main(cli_args) # erase handles its own ptype logic based on args.training/prediction
    else:
        if not (cli_args.training or cli_args.prediction):
            parser.error("Must specify a target pipeline type with --training or --prediction.")
        if not (cli_args.compile or cli_args.deploy or cli_args.schedule):
            parser.error("Must specify at least one action: --compile, --deploy, or --schedule.")

        # Process training pipeline actions if --training is specified
        if cli_args.training:
            logger.info("\n--- Processing Training Pipeline Actions ---")
            # Create a copy of args, ensuring only training is targeted for this main() call
            train_args = copy.deepcopy(cli_args)
            train_args.prediction = False 
            train_args.erase = False # erase is handled separately
            main(train_args)
        
        # Process prediction pipeline actions if --prediction is specified
        if cli_args.prediction:
            logger.info("\n--- Processing Prediction Pipeline Actions ---")
            # Create a copy of args, ensuring only prediction is targeted for this main() call
            pred_args = copy.deepcopy(cli_args)
            pred_args.training = False
            pred_args.erase = False # erase is handled separately
            main(pred_args)
            
        logger.info("\n--- Helper script finished all requested operations. ---")