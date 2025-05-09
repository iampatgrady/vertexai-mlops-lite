# test_render_sqlx.py
import sys
import os
import yaml
from jinja2 import Environment, FileSystemLoader, TemplateSyntaxError

def render_template(template_file, context_dict):
    """Renders a single Jinja template."""
    try:
        env = Environment(loader=FileSystemLoader(os.path.dirname(template_file)))
        template = env.get_template(os.path.basename(template_file))
        rendered_sql = template.render(context_dict)
        # Basic check for empty output which indicates a problem
        if not rendered_sql.strip():
             print(f"ERROR: Rendering {template_file} resulted in empty output.")
             return False
        print(f"--- Rendering SUCCEEDED for: {template_file} ---")
        # Optional: Print first few lines
        # print('\n'.join(rendered_sql.splitlines()[:15]))
        # print("...\n")
        return True
    except TemplateSyntaxError as e:
        print(f"!!! Jinja Syntax ERROR in {template_file}: {e} (Line: {e.lineno})")
        return False
    except Exception as e:
        print(f"!!! General ERROR rendering {template_file}: {e}")
        return False

if __name__ == "__main__":
    # Define paths relative to the script location or use absolute paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, "config.yaml") # Assumes config.yaml is in the same dir
    sp_metadata_path = os.path.join(script_dir, "sp_create_feature_metadata.sqlx")
    sp_data_path = os.path.join(script_dir, "sp_create_training_inference_data.sqlx")

    print(f"Loading config from: {config_path}")
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        if not config or 'gcp_project_id' not in config or 'bq_dataset_id' not in config:
            print("ERROR: config.yaml is missing required keys (gcp_project_id, bq_dataset_id).")
            sys.exit(1)
        # Create the context needed for the templates
        context = {
            'gcp_project_id': config['gcp_project_id'],
            'bq_dataset_id': config['bq_dataset_id']
        }
        print("Config loaded successfully.")
    except Exception as e:
        print(f"ERROR loading config file {config_path}: {e}")
        sys.exit(1)

    print("\nTesting Jinja rendering for Stored Procedures...")
    results = []
    results.append(render_template(sp_metadata_path, context))
    results.append(render_template(sp_data_path, context))

    print("\n--- Summary ---")
    if all(results):
        print("All SQLx templates rendered successfully (Jinja syntax OK).")
        print("NOTE: This does NOT validate the SQL logic itself against BigQuery.")
        sys.exit(0) # Success
    else:
        print("One or more SQLx templates failed to render. Check errors above.")
        sys.exit(1) # Failure