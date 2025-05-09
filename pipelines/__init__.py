# vai-basic/pipelines/__init__.py
import os
import yaml
import logging # Import logging here too for this file

# --- Configuration Loading ---
config_path_from_env = os.environ.get("TF_VAR_config_path", "config.yaml")
# Ensure path is relative to *this* file's directory before going up
config_path = os.path.join(os.path.dirname(__file__), "..", config_path_from_env)


config = None
base_image = None # Initialize

# --- Determine Base Image ---
# Priority 1: Environment variable set by Terraform during compilation
base_image_from_env = os.environ.get("BASE_IMAGE_URI")
if base_image_from_env:
    base_image = base_image_from_env
    logging.info(f"Using base image from environment variable: {base_image}")

# Priority 2: Load from config if not set by env
if not base_image:
    if os.path.exists(config_path):
        try:
            with open(config_path, encoding="utf-8") as fh:
                config = yaml.safe_load(fh)
            logging.info(f"Configuration loaded successfully from {config_path}")

            if config:
                # Define how to construct image URI from config (add this section to config.yaml if needed)
                img_config = config.get('docker_image', {})
                img_project = img_config.get('project_id', config.get('gcp_project_id'))
                img_region = img_config.get('region', config.get('gcp_region'))
                # Derive repo name from dataset ID if not specified
                img_repo = img_config.get('repository', f"{config.get('bq_dataset_id', 'default-dataset').replace('_', '-')}-images")
                img_name = img_config.get('name', 'pipeline-base')
                img_tag = img_config.get('tag', 'latest')

                if img_project and img_region and img_repo and img_name and img_tag:
                     base_image = f"{img_region}-docker.pkg.dev/{img_project}/{img_repo}/{img_name}:{img_tag}"
                     logging.info(f"Constructed base image from config/defaults: {base_image}")
                else:
                     logging.warning("Could not construct base_image from config - missing details.")

        except Exception as e:
            logging.error(f"Error loading config file {config_path} for base_image construction: {e}")
            # Don't raise SystemExit here, allow fallback
    else:
        logging.warning(f"Config file not found at {config_path} for base_image construction.")

# Priority 3: Final fallback (or raise error)
if not base_image:
    logging.error("Base image URI could not be determined from environment or config. Cannot proceed.")
    # Option: Use a default public image as last resort (less ideal)
    # base_image = "python:3.10"
    # logging.warning(f"Falling back to default public base image: {base_image}")
    raise ValueError("Essential base_image URI is missing.")


# Basic config validation (moved after potential config loading)
if not config or not config.get('gcp_project_id') or not config.get('bq_dataset_id'):
    # We might reach here if base_image was set by env var but config failed to load later
    # Re-attempt loading config just for validation if needed, or rely on Terraform's validation
    try:
        if not config:
             with open(config_path, encoding="utf-8") as fh:
                config = yaml.safe_load(fh)
    except Exception:
         pass # Ignore errors here, focus on missing keys

    if not config or not config.get('gcp_project_id') or not config.get('bq_dataset_id'):
       raise ValueError("Essential configuration keys (gcp_project_id, bq_dataset_id) missing in config.yaml")