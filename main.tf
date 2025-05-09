# ~/dev/vai-basic/main.tf
terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
    jinja = {
      source  = "NikolaLohinski/jinja"
      version = "~> 2.4"
    }
    random = {
      source  = "hashicorp/random"
      version = ">= 3.1.0"
    }
  }
}

variable "config_path" {
  type        = string
  default     = "config.yaml"
  description = "Path to the configuration YAML file, relative to the terraform module directory."
}

# --- Local Variables ---
locals {
  absolute_config_path = abspath(var.config_path)
  config_exists        = fileexists(local.absolute_config_path)
  yaml_config          = try(yamldecode(file(local.absolute_config_path)), null)
  config_hash          = local.config_exists ? filemd5(local.absolute_config_path) : ""
  base_config_loaded   = local.yaml_config != null
  sp_metadata_path    = abspath("${path.module}/sp_create_feature_metadata.sqlx")
  sp_data_path        = abspath("${path.module}/sp_create_training_inference_data.sqlx")
  dockerfile_path     = abspath("${path.module}/Dockerfile")
  requirements_path   = abspath("${path.module}/requirements.txt")
  sp_metadata_exists  = fileexists(local.sp_metadata_path)
  sp_data_exists      = fileexists(local.sp_data_path)
  dockerfile_exists   = fileexists(local.dockerfile_path)
  requirements_exists = fileexists(local.requirements_path)
  sp_metadata_hash  = local.sp_metadata_exists ? filemd5(local.sp_metadata_path) : ""
  sp_data_hash      = local.sp_data_exists ? filemd5(local.sp_data_path) : ""
  dockerfile_hash   = local.dockerfile_exists ? filemd5(local.dockerfile_path) : ""
  requirements_hash = local.requirements_exists ? filemd5(local.requirements_path) : ""
  pipelines_init_py_hash       = local.config_valid && fileexists("${path.module}/pipelines/__init__.py") ? filemd5("${path.module}/pipelines/__init__.py") : ""
  pipelines_training_py_hash   = local.config_valid && fileexists("${path.module}/pipelines/training.py") ? filemd5("${path.module}/pipelines/training.py") : ""
  pipelines_prediction_py_hash = local.config_valid && fileexists("${path.module}/pipelines/prediction.py") ? filemd5("${path.module}/pipelines/prediction.py") : ""
  components_bq_py_hash        = local.config_valid && fileexists("${path.module}/pipelines/components/bigquery_components.py") ? filemd5("${path.module}/pipelines/components/bigquery_components.py") : ""
  image_content_hash_string = join("-", compact([
    local.dockerfile_hash,
    local.requirements_hash,
    local.pipelines_init_py_hash,
    local.components_bq_py_hash,
  ]))
  dynamic_image_tag_suffix = substr(md5(local.image_content_hash_string), 0, 8)

  effective_docker_image_tag = local.config_valid ? (
    try(local.yaml_config.docker_image.tag, null) == null ? "sha-${local.dynamic_image_tag_suffix}" : local.yaml_config.docker_image.tag
  ) : "sha-${local.dynamic_image_tag_suffix}"

  base_keys_present = local.base_config_loaded && lookup(local.yaml_config, "gcp_project_id", null) != null && lookup(local.yaml_config, "gcp_region", null) != null && lookup(local.yaml_config, "bq_dataset_id", null) != null && lookup(local.yaml_config, "analytics_ga4_project_id", null) != null && lookup(local.yaml_config, "analytics_ga4_id", null) != null && lookup(local.yaml_config, "analytics_ga4_stream_id", null) != null && lookup(local.yaml_config, "feature_generation", null) != null && lookup(try(local.yaml_config.feature_generation, {}), "analysis_lookback_days", null) != null && lookup(try(local.yaml_config.feature_generation, {}), "top_n_string_values", null) != null && lookup(local.yaml_config, "bq_sp_params", null) != null && lookup(try(local.yaml_config.bq_sp_params, {}), "lookback_days", null) != null && lookup(try(local.yaml_config.bq_sp_params, {}), "lookahead_days", null) != null && lookup(local.yaml_config, "model", null) != null && lookup(try(local.yaml_config.model, {}), "type", null) != null && lookup(try(local.yaml_config.model, {}), "create_model_params", null) != null && lookup(try(local.yaml_config.model.create_model_params, {}), "model_type", null) != null && lookup(local.yaml_config, "training", null) != null && lookup(try(local.yaml_config.training, {}), "cron", null) != null && lookup(local.yaml_config, "prediction", null) != null && lookup(try(local.yaml_config.prediction, {}), "cron", null) != null
  training_dates_valid = local.base_keys_present && ((lookup(try(local.yaml_config.training, {}), "data_date_start", null) != null && lookup(try(local.yaml_config.training, {}), "data_date_end", null) != null && can(regex("^\\d{4}-\\d{2}-\\d{2}$", try(local.yaml_config.training.data_date_start, ""))) && can(regex("^\\d{4}-\\d{2}-\\d{2}$", try(local.yaml_config.training.data_date_end, "")))) || (lookup(try(local.yaml_config.training, {}), "data_date_start_days_ago", null) != null && lookup(try(local.yaml_config.training, {}), "data_date_start", null) == null && lookup(try(local.yaml_config.training, {}), "data_date_end", null) == null))
  prediction_dates_valid = local.base_keys_present && lookup(try(local.yaml_config.prediction, {}), "data_date_start_days_ago", null) != null
  model_type_is_bqml = local.base_config_loaded && lookup(try(local.yaml_config.model, {}), "type", null) == "BQML"
  config_valid = local.base_config_loaded && local.base_keys_present && local.training_dates_valid && local.prediction_dates_valid && local.sp_metadata_exists && local.sp_data_exists && local.model_type_is_bqml && local.dockerfile_exists && local.requirements_exists
  project_id             = local.config_valid ? local.yaml_config.gcp_project_id : "invalid-project"
  region                 = local.config_valid ? local.yaml_config.gcp_region : "invalid-region"
  dataset_id             = local.config_valid ? local.yaml_config.bq_dataset_id : "invalid-dataset"
  bq_location            = local.config_valid ? lookup(local.yaml_config, "bq_dataset_location", "US") : "US"
  repo_base_name         = replace(local.dataset_id, "_", "-")
  pipeline_repo_name     = "${local.repo_base_name}-pipelines"
  pipeline_bucket_name   = "${local.project_id}-${local.repo_base_name}-pipelines"
  docker_image_repo_name = local.config_valid ? try(local.yaml_config.docker_image.repository, "${local.repo_base_name}-images") : "${local.repo_base_name}-images"
  docker_image_name      = local.config_valid ? try(local.yaml_config.docker_image.name, "pipeline-base") : "pipeline-base"
  base_image_uri = local.config_valid ? "${local.region}-docker.pkg.dev/${local.project_id}/${local.docker_image_repo_name}/${local.docker_image_name}:${local.effective_docker_image_tag}" : "invalid-image-uri"
  has_training_config   = local.config_valid && lookup(try(local.yaml_config.training, {}), "cron", null) != null
  has_prediction_config = local.config_valid && lookup(try(local.yaml_config.prediction, {}), "cron", null) != null
  training_pipeline_name   = local.config_valid ? "mini-mlops-training-${local.repo_base_name}" : "invalid-training-pipeline"
  prediction_pipeline_name = local.config_valid ? "mini-mlops-prediction-${local.repo_base_name}" : "invalid-prediction-pipeline"
  training_yaml_file       = "training_pipeline_dynamic.yaml"
  prediction_yaml_file     = "prediction_pipeline_dynamic.yaml"

  config_error_message = !local.config_valid ? join(" ", compact([
    !local.config_exists ? "Config file '${var.config_path}' (abs: ${local.absolute_config_path}) not found." : "",
    local.config_exists && !local.base_config_loaded ? "Config file '${var.config_path}' is invalid YAML." : "",
    local.base_config_loaded && !local.base_keys_present ? "One or more required base keys missing (check gcp_*, bq_*, analytics_*, feature_generation.*, bq_sp_params.*, model.*, training.cron, prediction.cron)." : "",
    local.base_config_loaded && !local.training_dates_valid ? "Invalid 'training' date config: provide EITHER 'data_date_start'/'data_date_end' (YYYY-MM-DD) OR 'data_date_start_days_ago'." : "",
    local.base_config_loaded && !local.prediction_dates_valid ? "Invalid 'prediction' date config: must provide 'data_date_start_days_ago'." : "",
    !local.sp_metadata_exists ? "SP template file 'sp_create_feature_metadata.sqlx' not found." : "",
    !local.sp_data_exists ? "SP template file 'sp_create_training_inference_data.sqlx' not found." : "",
    local.base_config_loaded && !local.model_type_is_bqml ? "Model type in config is not 'BQML'." : "",
    !local.dockerfile_exists ? "Dockerfile not found at '${local.dockerfile_path}'." : "",
    !local.requirements_exists ? "requirements.txt not found at '${local.requirements_path}'." : "",
    ])) : "Configuration appears valid."
}

resource "null_resource" "config_validation" {
  triggers = {
    config_hash           = local.config_hash
    dockerfile_hash       = local.dockerfile_hash
    requirements_hash     = local.requirements_hash
    sp_metadata_hash      = local.sp_metadata_hash
    sp_data_hash          = local.sp_data_hash
    components_bq_py_hash = local.components_bq_py_hash
  }
  provisioner "local-exec" {
    command     = !local.config_valid ? "echo \"\n *** Configuration Error: ${local.config_error_message} ***\n\"; exit 1" : "echo 'Configuration validated successfully.'"
    interpreter = ["bash", "-c"]
  }
}

provider "google" {
  project = local.config_valid ? local.project_id : null
  region  = local.config_valid ? local.region : null
}

provider "jinja" {}

resource "google_project_service" "required_apis" {
  depends_on = [null_resource.config_validation]
  for_each   = local.config_valid ? toset([
    "aiplatform.googleapis.com", "artifactregistry.googleapis.com", "bigquery.googleapis.com",
    "cloudbuild.googleapis.com", "cloudresourcemanager.googleapis.com", "storage-component.googleapis.com",
    "iam.googleapis.com", "serviceusage.googleapis.com"
  ]) : toset([])

  project                    = local.project_id
  service                    = each.key
  disable_dependent_services = false
  disable_on_destroy         = false
}

resource "google_bigquery_dataset" "ml_dataset" {
  depends_on = [google_project_service.required_apis]
  count      = local.config_valid ? 1 : 0

  dataset_id                 = local.dataset_id
  project                    = local.project_id
  location                   = local.bq_location
  delete_contents_on_destroy = true
  description                = "Dataset for Mini VAI-MLOps Dynamic Features (${local.dataset_id})"
  labels = {
    "app"        = "mini-mlops"
    "created-by" = "terraform"
  }
}

data "jinja_template" "create_feature_metadata_sql" {
  depends_on = [null_resource.config_validation]
  count      = local.config_valid && local.sp_metadata_exists ? 1 : 0

  source {
    template  = file(local.sp_metadata_path)
    directory = path.module
  }
  context {
    type = "yaml"
    data = <<-EOT
      gcp_project_id: ${local.project_id}
      bq_dataset_id: ${local.dataset_id}
    EOT
  }
}

resource "google_bigquery_job" "create_sp_metadata" {
  depends_on = [google_bigquery_dataset.ml_dataset, data.jinja_template.create_feature_metadata_sql]
  count      = length(data.jinja_template.create_feature_metadata_sql) > 0 ? 1 : 0

  project  = local.project_id
  location = local.bq_location
  job_id   = "create_sp_metadata_${local.dataset_id}_${local.sp_metadata_hash}_${formatdate("YYYYMMDDhhmmss", timestamp())}"

  query {
    query              = one(data.jinja_template.create_feature_metadata_sql[*].result) # Use one() if count can be 0 or 1
    use_legacy_sql     = false
    write_disposition  = ""
    create_disposition = ""
  }
  labels = {
    "app"    = "mini-mlops"
    "action" = "create-sp-metadata"
  }
}

resource "null_resource" "wait_for_sp_metadata_creation" {
  depends_on = [google_bigquery_job.create_sp_metadata]
  count      = length(google_bigquery_job.create_sp_metadata) > 0 ? 1 : 0

  triggers = {
    job_id = one(google_bigquery_job.create_sp_metadata[*].job_id)
  }

  provisioner "local-exec" {
    command     = "echo 'Waiting for BQ job ${self.triggers.job_id} (SP Metadata) to complete...' && bq wait --project_id=${local.project_id} --location=${local.bq_location} --format=none ${self.triggers.job_id}"
    interpreter = ["bash", "-c"]
    environment = { CLOUDSDK_CORE_PROJECT = local.project_id }
  }
}

data "jinja_template" "create_training_inference_data_sql" {
  depends_on = [null_resource.config_validation]
  count      = local.config_valid && local.sp_data_exists ? 1 : 0

  source {
    template  = file(local.sp_data_path)
    directory = path.module
  }
  context {
    type = "yaml"
    data = <<-EOT
      gcp_project_id: ${local.project_id}
      bq_dataset_id: ${local.dataset_id}
    EOT
  }
}

resource "google_bigquery_job" "create_sp_data" {
  depends_on = [google_bigquery_job.create_sp_metadata, data.jinja_template.create_training_inference_data_sql]
  count      = length(data.jinja_template.create_training_inference_data_sql) > 0 ? 1 : 0

  project  = local.project_id
  location = local.bq_location
  job_id   = "create_sp_data_${local.dataset_id}_${local.sp_data_hash}_${formatdate("YYYYMMDDhhmmss", timestamp())}"

  query {
    query              = one(data.jinja_template.create_training_inference_data_sql[*].result)
    use_legacy_sql     = false
    write_disposition  = ""
    create_disposition = ""
  }
  labels = {
    "app"    = "mini-mlops"
    "action" = "create-sp-data"
  }
}

resource "null_resource" "wait_for_sp_data_creation" {
  depends_on = [google_bigquery_job.create_sp_data]
  count      = length(google_bigquery_job.create_sp_data) > 0 ? 1 : 0

  triggers = {
    job_id = one(google_bigquery_job.create_sp_data[*].job_id)
  }

  provisioner "local-exec" {
    command     = "echo 'Waiting for BQ job ${self.triggers.job_id} (SP Data) to complete...' && bq wait --project_id=${local.project_id} --location=${local.bq_location} --format=none ${self.triggers.job_id}"
    interpreter = ["bash", "-c"]
    environment = { CLOUDSDK_CORE_PROJECT = local.project_id }
  }
}

# --- Cloud Storage for Pipeline Root ---
resource "google_storage_bucket" "pipeline_root_bucket" {
  depends_on = [google_project_service.required_apis]
  count      = local.config_valid ? 1 : 0

  project                     = local.project_id
  name                        = local.pipeline_bucket_name
  location                    = local.region
  uniform_bucket_level_access = true
  force_destroy               = true
  labels = {
    "app"     = "mini-mlops"
    "purpose" = "vertex-pipelines-root"
  }
}

data "google_project" "project_details" {
  depends_on = [null_resource.config_validation]
  count      = local.config_valid ? 1 : 0
  project_id = local.project_id
}

locals {
  project_number       = try(one(data.google_project.project_details[*].number), "invalid-project-number")
  compute_engine_sa  = "serviceAccount:${coalesce(local.project_number, "0")}-compute@developer.gserviceaccount.com"
  vertex_ai_agent_sa = "serviceAccount:service-${coalesce(local.project_number, "0")}@gcp-sa-aiplatform.iam.gserviceaccount.com"
  cloud_build_sa     = "serviceAccount:${coalesce(local.project_number, "0")}@cloudbuild.gserviceaccount.com"
}

resource "google_storage_bucket_iam_member" "compute_sa_pipelines_bucket_access" {
  depends_on = [google_storage_bucket.pipeline_root_bucket, data.google_project.project_details]
  count      = local.config_valid ? 1 : 0

  bucket = one(google_storage_bucket.pipeline_root_bucket[*].name)
  role   = "roles/storage.objectAdmin"
  member = local.compute_engine_sa
}

resource "google_artifact_registry_repository" "kfp_repo" {
  depends_on = [google_project_service.required_apis]
  count      = local.config_valid ? 1 : 0

  project       = local.project_id
  location      = local.region
  repository_id = local.pipeline_repo_name
  description   = "KFP Pipeline Templates for Mini VAI-MLOps Dynamic (${local.dataset_id})"
  format        = "KFP"
  labels        = { "app" = "mini-mlops", "purpose" = "kfp-templates" }
}

resource "google_artifact_registry_repository" "docker_repo" {
  depends_on = [google_project_service.required_apis]
  count      = local.config_valid ? 1 : 0

  project       = local.project_id
  location      = local.region
  repository_id = local.docker_image_repo_name
  description   = "Docker Images for Mini VAI-MLOps Dynamic (${local.dataset_id})"
  format        = "DOCKER"
  labels        = { "app" = "mini-mlops", "purpose" = "docker-images" }
}

resource "google_artifact_registry_repository_iam_member" "compute_sa_ar_docker_reader" {
  depends_on = [google_artifact_registry_repository.docker_repo, data.google_project.project_details]
  count      = local.config_valid ? 1 : 0

  project    = local.project_id
  location   = local.region
  repository = one(google_artifact_registry_repository.docker_repo[*].repository_id)
  role       = "roles/artifactregistry.reader"
  member     = local.compute_engine_sa
}

resource "google_artifact_registry_repository_iam_member" "vertex_agent_ar_docker_reader" {
  depends_on = [google_artifact_registry_repository.docker_repo, data.google_project.project_details]
  count      = local.config_valid ? 1 : 0

  project    = local.project_id
  location   = local.region
  repository = one(google_artifact_registry_repository.docker_repo[*].repository_id)
  role       = "roles/artifactregistry.reader"
  member     = local.vertex_ai_agent_sa
}

resource "google_artifact_registry_repository_iam_member" "cloud_build_ar_docker_writer" {
  depends_on = [google_artifact_registry_repository.docker_repo, data.google_project.project_details]
  count      = local.config_valid ? 1 : 0

  project    = local.project_id
  location   = local.region
  repository = one(google_artifact_registry_repository.docker_repo[*].repository_id)
  role       = "roles/artifactregistry.writer"
  member     = local.cloud_build_sa
}

resource "null_resource" "build_push_image_gcb" {
  depends_on = [
    google_artifact_registry_repository.docker_repo,
    google_artifact_registry_repository_iam_member.cloud_build_ar_docker_writer,
    null_resource.config_validation
  ]
  count = local.config_valid ? 1 : 0

  triggers = {
    image_uri              = local.base_image_uri # Primary trigger
    dockerfile_hash        = local.dockerfile_hash
    requirements_hash      = local.requirements_hash
    pipelines_init_py_hash = local.pipelines_init_py_hash
    components_bq_py_hash  = local.components_bq_py_hash
    config_hash            = local.config_hash # If config changes influence image name/repo/tag
  }

  provisioner "local-exec" {
    command = <<EOT
      echo "Effective Docker Image Tag: ${local.effective_docker_image_tag}"
      echo "Submitting Cloud Build job to build and push ${self.triggers.image_uri} from context ${path.module}..."
      gcloud builds submit --project ${local.project_id} --region=${local.region} --tag ${self.triggers.image_uri} --quiet ${path.module}
      if [ $? -ne 0 ]; then
        echo "ERROR: Cloud Build submission FAILED for image ${self.triggers.image_uri}."
        exit 1
      fi
      echo "Cloud Build job for ${self.triggers.image_uri} submitted successfully (check GCP console for status)."
      EOT
    interpreter = ["bash", "-c"]
    environment = { CLOUDSDK_CORE_PROJECT = local.project_id }
    on_failure  = fail
  }
}

resource "null_resource" "compile_training" {
  depends_on = [
    null_resource.build_push_image_gcb,
    null_resource.wait_for_sp_data_creation
  ]
  count = local.config_valid && local.has_training_config ? 1 : 0

  triggers = {
    config_hash              = local.config_hash
    script_hash              = filemd5("${path.module}/terraform/helpers.py")
    base_image_uri_trigger   = local.base_image_uri
    sp_metadata_hash         = local.sp_metadata_hash
    sp_data_hash             = local.sp_data_hash
    training_py_hash         = local.pipelines_training_py_hash
    components_py_hash       = local.components_bq_py_hash
    init_py_hash             = local.pipelines_init_py_hash
    yaml_name_hash           = md5(local.training_yaml_file)
  }

  provisioner "local-exec" {
    command = <<EOT
      echo "Cleaning Python cache in ${path.module}/pipelines..."
      find "${path.module}/pipelines" -type d -name '__pycache__' -exec rm -rf {} +
      find "${path.module}/pipelines" -type f -name '*.pyc' -delete
      echo "Python cache cleaned."
      python "${path.module}/terraform/helpers.py" --training --compile --config_path "${local.absolute_config_path}"
    EOT
    environment = {
      BASE_IMAGE_URI     = local.base_image_uri
      TF_VAR_config_path = var.config_path
    }
    interpreter = ["bash", "-c"] # Ensure bash is used for find/rm
  }
}

resource "null_resource" "deploy_training" {
  depends_on = [
    null_resource.compile_training,
    google_artifact_registry_repository.kfp_repo
  ]
  count = length(null_resource.compile_training) > 0 ? 1 : 0

  triggers = {
    compiled_trigger = one(null_resource.compile_training[*].id)
    repo_name        = one(google_artifact_registry_repository.kfp_repo[*].name)
  }

  provisioner "local-exec" {
    command = "python ${path.module}/terraform/helpers.py --training --deploy --config_path ${local.absolute_config_path}"
    environment = {
      TF_VAR_config_path = var.config_path
    }
  }
}

resource "null_resource" "schedule_training" {
  depends_on = [
    null_resource.deploy_training,
    google_storage_bucket_iam_member.compute_sa_pipelines_bucket_access,
    google_artifact_registry_repository_iam_member.compute_sa_ar_docker_reader,
    google_artifact_registry_repository_iam_member.vertex_agent_ar_docker_reader,
    null_resource.wait_for_sp_data_creation
  ]
  count = length(null_resource.deploy_training) > 0 ? 1 : 0

  triggers = {
    config_hash      = local.config_hash
    deployed_trigger = one(null_resource.deploy_training[*].id)
    project_id       = local.project_id
    region           = local.region
    pipeline_name    = local.training_pipeline_name 
  }

  provisioner "local-exec" {
    command = "python ${path.module}/terraform/helpers.py --training --schedule --config_path ${local.absolute_config_path}"
    environment = {
      TF_VAR_config_path = var.config_path
    }
  }

  provisioner "local-exec" {
    when    = destroy
    command = "python ${path.module}/terraform/helpers.py --erase --training"
    environment = {
      PIPELINE_PROJECT_ID    = self.triggers.project_id
      PIPELINE_REGION        = self.triggers.region
      PIPELINE_NAME          = self.triggers.pipeline_name 
    }
    on_failure = continue
  }
}

resource "null_resource" "compile_prediction" {
  depends_on = [
    null_resource.build_push_image_gcb,
    null_resource.wait_for_sp_data_creation
  ]
  count = local.config_valid && local.has_prediction_config ? 1 : 0

  triggers = {
    config_hash              = local.config_hash
    script_hash              = filemd5("${path.module}/terraform/helpers.py")
    base_image_uri_trigger   = local.base_image_uri
    sp_metadata_hash         = local.sp_metadata_hash
    sp_data_hash             = local.sp_data_hash
    prediction_py_hash       = local.pipelines_prediction_py_hash
    components_py_hash       = local.components_bq_py_hash
    init_py_hash             = local.pipelines_init_py_hash
    yaml_name_hash           = md5(local.prediction_yaml_file)
  }

  provisioner "local-exec" {
    command = <<EOT
      echo "Cleaning Python cache in ${path.module}/pipelines..."
      find "${path.module}/pipelines" -type d -name '__pycache__' -exec rm -rf {} +
      find "${path.module}/pipelines" -type f -name '*.pyc' -delete
      echo "Python cache cleaned."
      python "${path.module}/terraform/helpers.py" --prediction --compile --config_path "${local.absolute_config_path}"
    EOT
    environment = {
      BASE_IMAGE_URI     = local.base_image_uri
      TF_VAR_config_path = var.config_path
    }
    interpreter = ["bash", "-c"] # Ensure bash is used for find/rm
  }
}

resource "null_resource" "deploy_prediction" {
  depends_on = [
    null_resource.compile_prediction,
    google_artifact_registry_repository.kfp_repo
  ]
  count = length(null_resource.compile_prediction) > 0 ? 1 : 0

  triggers = {
    compiled_trigger = one(null_resource.compile_prediction[*].id)
    repo_name        = one(google_artifact_registry_repository.kfp_repo[*].name)
  }

  provisioner "local-exec" {
    command = "python ${path.module}/terraform/helpers.py --prediction --deploy --config_path ${local.absolute_config_path}"
    environment = {
      TF_VAR_config_path = var.config_path
    }
  }
}

resource "null_resource" "schedule_prediction" {
  depends_on = [
    null_resource.deploy_prediction,
    google_storage_bucket_iam_member.compute_sa_pipelines_bucket_access,
    google_artifact_registry_repository_iam_member.compute_sa_ar_docker_reader,
    google_artifact_registry_repository_iam_member.vertex_agent_ar_docker_reader,
    null_resource.wait_for_sp_data_creation
  ]
  count = length(null_resource.deploy_prediction) > 0 ? 1 : 0

  triggers = {
    config_hash      = local.config_hash
    deployed_trigger = one(null_resource.deploy_prediction[*].id)
    project_id       = local.project_id
    region           = local.region
    pipeline_name    = local.prediction_pipeline_name # This is correct for prediction
  }

  provisioner "local-exec" {
    command = "python ${path.module}/terraform/helpers.py --prediction --schedule --config_path ${local.absolute_config_path}"
    environment = {
      TF_VAR_config_path = var.config_path
    }
  }

  provisioner "local-exec" {
    when    = destroy
    command = "python ${path.module}/terraform/helpers.py --erase --prediction"
    environment = {
      PIPELINE_PROJECT_ID      = self.triggers.project_id
      PIPELINE_REGION          = self.triggers.region
      PIPELINE_NAME            = self.triggers.pipeline_name # Correctly passes prediction pipeline name
    }
    on_failure = continue
  }
}

output "configuration_validity_status" {
  value       = local.config_valid ? "Valid" : "INVALID - Check 'configuration_error_details'"
  description = "Indicates if essential config values and files were found and valid."
}
output "configuration_error_details" {
  value       = local.config_error_message
  description = "Specific errors found during configuration validation, if any."
  sensitive   = true
}
output "gcp_project_id" {
  value       = local.config_valid ? local.project_id : "N/A (Invalid Config)"
  description = "GCP Project ID used for deployment."
}
output "bigquery_dataset_id" {
  value       = length(google_bigquery_dataset.ml_dataset) > 0 ? one(google_bigquery_dataset.ml_dataset[*].id) : "Dataset not created."
  description = "The fully qualified ID of the created BigQuery dataset."
}
output "kfp_repository_name" {
  value       = length(google_artifact_registry_repository.kfp_repo) > 0 ? one(google_artifact_registry_repository.kfp_repo[*].name) : "KFP repository not created."
  description = "The full name of the Artifact Registry repository for KFP templates."
}
output "docker_repository_name" {
  value       = length(google_artifact_registry_repository.docker_repo) > 0 ? one(google_artifact_registry_repository.docker_repo[*].name) : "Docker repository not created."
  description = "The full name of the Artifact Registry repository for Docker images."
}
output "base_image_uri_used" {
  value       = local.config_valid ? local.base_image_uri : "Image URI not determined (invalid config)."
  description = "The full URI of the Docker image built/pushed by Cloud Build. This will change if image content changes."
}
output "training_pipeline_name" {
  value       = local.config_valid && local.has_training_config ? local.training_pipeline_name : "N/A (Not configured or invalid config)"
  description = "Name of the training pipeline in Vertex AI."
}
output "prediction_pipeline_name" {
  value       = local.config_valid && local.has_prediction_config ? local.prediction_pipeline_name : "N/A (Not configured or invalid config)"
  description = "Name of the prediction pipeline in Vertex AI."
}