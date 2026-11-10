# BigQuery IDR Deployment with Terraform
#
# Provisions infrastructure for serverless scheduled IDR execution:
# - BigQuery Datasets: idr_meta, idr_work, idr_out
# - Service Account with BigQuery JobUser & DataEditor permissions
# - Cloud Function (2nd Gen) executing IDRRunner / BigQueryAdapter
# - Cloud Scheduler for hourly incremental runs and weekly full refreshes

terraform {
  required_version = ">= 1.5.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP Region for Cloud Function & Scheduler"
  type        = string
  default     = "us-central1"
}

variable "bq_location" {
  description = "BigQuery dataset location"
  type        = string
  default     = "US"
}

variable "schedule" {
  description = "Cron schedule for incremental IDR runs"
  type        = string
  default     = "0 * * * *"  # Every hour
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ============================================
# 1. SERVICE ACCOUNT & IAM
# ============================================

resource "google_service_account" "idr_runner" {
  account_id   = "idr-runner"
  display_name = "IDR Pipeline Service Account"
}

resource "google_project_iam_member" "bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.idr_runner.email}"
}

# ============================================
# 2. BIGQUERY DATASETS
# ============================================

resource "google_bigquery_dataset" "idr_meta" {
  dataset_id = "idr_meta"
  location   = var.bq_location

  access {
    role          = "WRITER"
    user_by_email = google_service_account.idr_runner.email
  }
}

resource "google_bigquery_dataset" "idr_work" {
  dataset_id = "idr_work"
  location   = var.bq_location

  access {
    role          = "WRITER"
    user_by_email = google_service_account.idr_runner.email
  }
}

resource "google_bigquery_dataset" "idr_out" {
  dataset_id = "idr_out"
  location   = var.bq_location

  access {
    role          = "WRITER"
    user_by_email = google_service_account.idr_runner.email
  }
}

# ============================================
# 3. CLOUD FUNCTION (IDR RUNNER)
# ============================================

resource "google_storage_bucket" "function_bucket" {
  name                        = "${var.project_id}-idr-artifacts"
  location                    = var.region
  uniform_bucket_level_access = true
}

resource "google_cloudfunctions2_function" "idr_runner" {
  name        = "idr-runner"
  location    = var.region
  description = "Executes sql-identity-resolution against BigQuery datasets"

  build_config {
    runtime     = "python311"
    entry_point = "main"
    source {
      storage_source {
        bucket = google_storage_bucket.function_bucket.name
        object = "idr_function.zip"
      }
    }
  }

  service_config {
    max_instance_count = 1
    min_instance_count = 0
    available_memory   = "2048M"
    timeout_seconds    = 540

    environment_variables = {
      IDR_PLATFORM = "bigquery"
      IDR_PROJECT  = var.project_id
      IDR_LOCATION = var.bq_location
    }

    service_account_email = google_service_account.idr_runner.email
  }
}

# ============================================
# 4. CLOUD SCHEDULER JOBS
# ============================================

resource "google_cloud_scheduler_job" "idr_hourly_incremental" {
  name        = "idr-hourly-incremental"
  description = "Trigger hourly incremental IDR resolution"
  schedule    = var.schedule
  time_zone   = "UTC"

  http_target {
    uri         = "${google_cloudfunctions2_function.idr_runner.service_config[0].uri}?mode=INCR"
    http_method = "POST"

    oidc_token {
      service_account_email = google_service_account.idr_runner.email
    }
  }

  retry_config {
    retry_count = 1
  }
}

resource "google_cloud_scheduler_job" "idr_weekly_full" {
  name        = "idr-weekly-full"
  description = "Trigger weekly full graph IDR resolution"
  schedule    = "0 2 * * 0"  # Sunday 02:00 UTC
  time_zone   = "UTC"

  http_target {
    uri         = "${google_cloudfunctions2_function.idr_runner.service_config[0].uri}?mode=FULL"
    http_method = "POST"

    oidc_token {
      service_account_email = google_service_account.idr_runner.email
    }
  }

  retry_config {
    retry_count = 1
  }
}

# ============================================
# 5. OUTPUTS
# ============================================

output "service_account_email" {
  description = "Service account running IDR pipeline"
  value       = google_service_account.idr_runner.email
}

output "function_url" {
  description = "Trigger URL for IDR Cloud Function"
  value       = google_cloudfunctions2_function.idr_runner.service_config[0].uri
}

output "hourly_scheduler_job" {
  description = "Hourly Cloud Scheduler Job name"
  value       = google_cloud_scheduler_job.idr_hourly_incremental.name
}
