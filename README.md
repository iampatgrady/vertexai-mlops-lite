# vai-basic: Minimalist Dynamic Feature BQML MLOps

`vai-basic` is a starter MLOps framework on Google Cloud Platform, focusing on automating the lifecycle of a BigQuery ML model that uses dynamic features derived from Google Analytics 4 (GA4) data. It provides infrastructure provisioning via Terraform and pipeline orchestration using Vertex AI Pipelines (KFP v2), linked via a Python helper script.

This project is designed to be minimalist, demonstrating a specific dynamic feature pattern (BQ SPs + metadata linking) and providing a foundation for extension.

## Prerequisites

To deploy and run this framework from your local machine, you need the following software installed:

1.  **Terraform:** Used to provision GCP infrastructure.
    *   [Installation Guide](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli)
2.  **Python 3.8+:** Used for the KFP pipelines and the Terraform helper script.
    *   [Installation Guide](https://www.python.org/downloads/)
3.  **Google Cloud CLI (`gcloud`):** Used for authentication and potentially submitting Cloud Build jobs directly (though Terraform handles this automatically).
    *   [Installation Guide](https://cloud.google.com/sdk/docs/install)

Alternatively, you can use **Google Cloud Shell**, which typically comes pre-installed with Terraform, Python, and `gcloud`.

## Authentication

You need to authenticate `gcloud` and set up Application Default Credentials (ADC) so that Terraform and the Python client libraries (like `google-cloud-bigquery`, `google-cloud-aiplatform`) can interact with your GCP project.

1.  **Log in with `gcloud` (for general CLI commands and user identity):**
    ```bash
    gcloud auth login
    ```
    Follow the prompts to log in via your web browser.

2.  **Set up Application Default Credentials (for tools and client libraries):**
    ```bash
    gcloud auth application-default login
    ```
    This creates credentials that Terraform and Python libraries will automatically pick up.

3.  **Configure your default GCP project:**
    ```bash
    gcloud config set project YOUR_GCP_PROJECT_ID
    ```
    Replace `YOUR_GCP_PROJECT_ID` with your actual Google Cloud project ID.

## Installation

1.  **Clone the repository:**
    ```bash
    git clone <repository_url>
    cd <repository_directory>/vai-basic
    ```
    (Replace `<repository_url>` and `<repository_directory>` with the actual path to your cloned repo).

2.  **[Optional] Create and activate a Python Virtual Environment:**
    It's highly recommended to use a virtual environment to manage Python dependencies.
    ```bash
    python3 -m venv venv
    source venv/bin/activate # On Linux/macOS
    # venv\Scripts\activate # On Windows
    ```

3.  **Install Python dependencies:**
    Navigate to the `vai-basic` directory (where `requirements.txt` is located) and install the necessary libraries:
    ```bash
    pip install -r requirements.txt
    ```

4.  **Initialize Terraform:**
    Navigate to the `vai-basic` directory (where `main.tf` is located) and initialize Terraform. This downloads the required providers.
    ```bash
    terraform init
    ```

## Configuration

The framework's behavior and deployment targets are controlled by two types of files:

1.  **`config.yaml`:** This is the primary configuration file.
    *   It is located at `./vai-basic/config.yaml`.
    *   **You MUST edit this file** and replace the placeholder values (like `gcp_project_id`, `analytics_ga4_id`, etc.) with your actual GCP and GA4 details.
    *   Review each section (`gcp_*`, `bq_*`, `analytics_*`, `feature_generation`, `bq_sp_params`, `model`, `training`, `prediction`) and adjust values according to your needs (e.g., dataset name, region, GA4 properties, model hyperparameters, cron schedules).
    *   Pay close attention to the date configuration under `training` and `prediction`. The current setup requires `data_date_start_days_ago` for prediction and allows either fixed dates or `data_date_start_days_ago` for training.

2.  **`.sqlx` Files:** These files contain the SQL code for the BigQuery Stored Procedures, templated using Jinja.
    *   `./vai-basic/sp_create_feature_metadata.sqlx`
    *   `./vai-basic/sp_create_training_inference_data.sqlx`
    *   These files are automatically processed by Terraform using the `jinja` provider to inject basic configuration (project and dataset IDs) before deploying the SPs to BigQuery.
    *   **You generally do not need to edit these files** unless you intend to change the core logic of how features are analyzed or data is prepared.
    *   If you *do* modify the `.sqlx` files, you can perform a basic check of the Jinja syntax (not the SQL logic) using the included test script:
        ```bash
        python ./vai-basic/test_render_sqlx.py
        ```

Make sure your `config.yaml` is correctly filled out before proceeding to deployment.

## Deployment

Once your configuration is complete and Terraform is initialized, you can deploy the infrastructure and set up the pipelines using Terraform's apply command.

1.  **Review the deployment plan (Recommended):**
    ```bash
    terraform plan
    ```
    This command shows you exactly which resources Terraform will create, modify, or destroy. Review the output carefully to ensure it matches your expectations based on the `config.yaml`.

2.  **Apply the deployment:**
    ```bash
    terraform apply
    ```
    Terraform will:
    *   Provision GCP resources (BigQuery dataset, GCS bucket, Artifact Registries, IAM bindings).
    *   Deploy the BigQuery Stored Procedures.
    *   Trigger a Cloud Build job to build your Docker image and push it to Artifact Registry.
    *   Run the Python helper script (`terraform/helpers.py`) to:
        *   Compile the KFP pipelines (`training.py`, `prediction.py`) into YAML.
        *   Upload the pipeline YAMLs to the KFP Artifact Registry.
        *   Create/update Vertex AI Pipeline Schedules based on the cron jobs specified in `config.yaml`.

    Confirm the apply by typing `yes` when prompted.

The `terraform apply` process can take several minutes as it waits for Cloud Build jobs and BigQuery SP creation jobs to complete.

After successful deployment, you can verify the resources in the Google Cloud Console:
*   BigQuery: Check the dataset and Stored Procedures.
*   Artifact Registry: Check the Docker and KFP repositories.
*   Vertex AI -> Pipelines -> Schedules: Verify the created pipeline schedules.

## Cleanup

To remove all resources created by this Terraform configuration, including the Vertex AI Pipeline Schedules, use the destroy command.

```bash
terraform destroy
```
Review the plan and confirm by typing yes when prompted. The helper script's destroy provisioner will run automatically to clean up the schedules before Terraform deletes the rest of the infrastructure.  
