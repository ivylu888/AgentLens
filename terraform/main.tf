# ==============================================================================
# AgentLens - Sprint 1: Zero-Touch Provisioning Infrastructure
# ==============================================================================

terraform {
  required_version = ">= 1.3.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
    google-beta = {
      source  = "hashicorp/google-beta"
      version = "~> 5.0"
    }
    time = {
      source  = "hashicorp/time"
      version = "~> 0.9"
    }
  }
}

provider "google" {
  project = "agentlens-496022"
  region  = "us-central1"
}

# ------------------------------------------------------------------------------
# 0. Enable Required APIs FIRST (The root cause of the previous 400 error)
# ------------------------------------------------------------------------------

resource "google_project_service" "pubsub_api" {
  project            = "agentlens-496022"
  service            = "pubsub.googleapis.com"
  disable_on_destroy = false # Prevent disabling APIs during destroy to avoid impacting other resources
}

resource "google_project_service" "bigquery_api" {
  project            = "agentlens-496022"
  service            = "bigquery.googleapis.com"
  disable_on_destroy = false
}

# Wait for API enablement to propagate globally across GCP infrastructure
resource "time_sleep" "wait_for_apis" {
  depends_on = [
    google_project_service.pubsub_api,
    google_project_service.bigquery_api,
  ]
  create_duration = "30s" # API enablement requires a longer propagation window than Service Accounts
}

# ------------------------------------------------------------------------------
# 1. Service Identity (Must occur AFTER APIs are fully enabled)
# ------------------------------------------------------------------------------

resource "google_project_service_identity" "pubsub_identity" {
  provider   = google-beta
  service    = "pubsub.googleapis.com"
  project    = "agentlens-496022"
  depends_on = [time_sleep.wait_for_apis] # Explicit dependency on API propagation
}

# Wait for the newly created Service Account to be recognizable by the IAM mesh
resource "time_sleep" "wait_for_pubsub_sa" {
  depends_on      = [google_project_service_identity.pubsub_identity]
  create_duration = "10s"
}

resource "google_pubsub_topic_iam_binding" "pubsub_dlq_publisher" {
  topic = google_pubsub_topic.span_dlq_topic.name
  role  = "roles/pubsub.publisher"
  members = [
    "serviceAccount:${google_project_service_identity.pubsub_identity.email}"
  ]
  depends_on = [time_sleep.wait_for_pubsub_sa] # Wait for the SA to fully exist before binding roles
}

# ------------------------------------------------------------------------------
# 2. BigQuery Dataset
# ------------------------------------------------------------------------------
resource "google_bigquery_dataset" "agentlens_dataset" {
  dataset_id                 = "agentlens_dw"
  friendly_name              = "AgentLens Data Warehouse"
  description                = "Repository for Medallion architecture layers (Silver/Gold)"
  location                   = "US"
  delete_contents_on_destroy = true
  depends_on                 = [time_sleep.wait_for_apis] # Ensure BigQuery API is ready before dataset creation
}

# ------------------------------------------------------------------------------
# 3. GCS Bucket
# ------------------------------------------------------------------------------
resource "google_storage_bucket" "agentlens_lake" {
  name          = "agentlens-lake-wcl01"
  location      = "US"
  storage_class = "STANDARD"
  force_destroy = true

  lifecycle_rule {
    condition {
      age            = 7
      matches_prefix = ["spans/landing/"]
    }
    action { 
      type = "Delete" 
    }
  }
}

# ------------------------------------------------------------------------------
# 4. Pub/Sub Topics & Subscriptions
# ------------------------------------------------------------------------------

resource "google_pubsub_topic" "span_dlq_topic" {
  name       = "agentlens-span-dlq"
  depends_on = [time_sleep.wait_for_apis]
}

resource "google_pubsub_subscription" "span_dlq_sub" {
  name       = "agentlens-span-dlq-sub"
  topic      = google_pubsub_topic.span_dlq_topic.name
  depends_on = [time_sleep.wait_for_apis]
}

resource "google_pubsub_topic" "span_main_topic" {
  name       = "agentlens-span-main"
  depends_on = [time_sleep.wait_for_apis]
}

resource "google_pubsub_subscription" "span_main_sub" {
  name                 = "agentlens-span-main-sub"
  topic                = google_pubsub_topic.span_main_topic.name
  ack_deadline_seconds = 60

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.span_dlq_topic.id
    max_delivery_attempts = 5
  }

  depends_on = [google_pubsub_topic_iam_binding.pubsub_dlq_publisher]
}