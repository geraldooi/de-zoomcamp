terraform {
  required_version = ">= 1.0"
  backend "local" {}
  required_providers {
    google = {
      source  = "hashicorp/google"
    }
  }
}

provider "google" {
  project = var.project
  region = var.region
}

# DWH
resource "google_bigquery_dataset" "dataset1" {
  dataset_id    = "dbt_gerald"
  friendly_name = "DBT Gerald"
  project       = var.project
  location      = var.region
}

resource "google_bigquery_dataset" "dataset2" {
  dataset_id    = "production"
  friendly_name = "Production"
  project       = var.project
  location      = var.region
}

resource "google_bigquery_dataset" "dataset3" {
  dataset_id    = "staging"
  friendly_name = "Staging"
  project       = var.project
  location      = var.region
}

resource "google_bigquery_dataset" "dataset4" {
  dataset_id    = "trip_data_all"
  friendly_name = "Trip Data All"
  project       = var.project
  location      = var.region
}