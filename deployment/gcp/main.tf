# BigQuery IDR Deployment with Terraform
#
# This creates:
# - Required datasets
# - Cloud Function for running IDR
# - Cloud Scheduler for scheduling runs
# - Service account with minimal permissions

terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "us-central1"
}

variable "schedule" {
  description = "Cron schedule for IDR runs"
  type        = string
  default     = "0 * * * *"  # Every hour
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ============================================
# SERVICE ACCOUNT
# ============================================

resource "google_service_account" "idr_runner" {
  account_id   = "idr-runner"
  display_name = "IDR Runner Service Account"
}

resource "google_project_iam_member" "bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.idr_runner.email}"
}

# ============================================
# DATASETS
# ============================================

resource "google_bigquery_dataset" "idr_meta" {
  dataset_id = "idr_meta"
  location   = "US"

  access {
    role          = "WRITER"
    user_by_email = google_service_account.idr_runner.email
  }
}

resource "google_bigquery_dataset" "idr_work" {
  dataset_id = "idr_work"
  location   = "US"

  access {
    role          = "WRITER"
    user_by_email = google_service_account.idr_runner.email
  }
}

resource "google_bigquery_dataset" "idr_out" {
  dataset_id = "idr_out"
  location   = "US"

  access {
    role          = "WRITER"
    user_by_email = google_service_account.idr_runner.email
  }
}

# ============================================
# CLOUD FUNCTION
# ============================================

resource "google_storage_bucket" "function_bucket" {
  name     = "${var.project_id}-idr-functions"
  location = var.region
}

# Note: You need to upload your function code to this bucket
# data "archive_file" "function_zip" {
#   type        = "zip"
#   source_dir  = "${path.module}/../../sql/bigquery"
#   output_path = "${path.module}/function.zip"
# }

resource "google_cloudfunctions2_function" "idr_runner" {
  name     = "idr-runner"
  location = var.region

  build_config {
    runtime     = "python311"
    entry_point = "run_idr"
    source {
      storage_source {
        bucket = google_storage_bucket.function_bucket.name
        object = "idr_function.zip"  # Upload this separately
      }
    }
  }

  service_config {
    max_instance_count = 1
    min_instance_count = 0
    available_memory   = "2048M"
    timeout_seconds    = 540

    environment_variables = {
      GCP_PROJECT = var.project_id
    }

    service_account_email = google_service_account.idr_runner.email
  }
}

# ============================================
# CLOUD SCHEDULER
# ============================================

resource "google_cloud_scheduler_job" "idr_hourly" {
  name      = "idr-hourly"
  schedule  = var.schedule
  time_zone = "UTC"

  http_target {
    uri         = google_cloudfunctions2_function.idr_runner.service_config[0].uri
    http_method = "GET"

    oidc_token {
      service_account_email = google_service_account.idr_runner.email
    }
  }

  retry_config {
    retry_count = 1
  }
}

resource "google_cloud_scheduler_job" "idr_weekly_full" {
  name      = "idr-weekly-full"
  schedule  = "0 2 * * 0"  # Sunday 2am
  time_zone = "UTC"

  http_target {
    uri         = "${google_cloudfunctions2_function.idr_runner.service_config[0].uri}?mode=FULL"
    http_method = "GET"

    oidc_token {
      service_account_email = google_service_account.idr_runner.email
    }
  }
}

# ============================================
# OUTPUTS
# ============================================

output "service_account_email" {
  value = google_service_account.idr_runner.email
}

output "function_url" {
  value = google_cloudfunctions2_function.idr_runner.service_config[0].uri
}

output "scheduler_job_name" {
  value = google_cloud_scheduler_job.idr_hourly.name
}
