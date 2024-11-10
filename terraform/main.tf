# This main.tf creates a VM and a GCS bucket with the following condigurations:

# - Virtual machine name: de-zoomcamp-via-terraform
# - Machine type: e2-standard-4
# - Memory: 16GB
# - CPU platform: Intel Broadwell
# - Architecture: x86/64
# - Regionnorthamerica-northeast1-b
# - OS: Ubuntu
# - OS: Ubuntu Version 20.04LTS
# - Storage Size: 50 GB
# - Boot disk type: Balanced persistent
# - Boot disk architecture: x86/64
# - Bucket name: de-zoomcamp-bucket-terraform-test
# - Bucket location type: US (multiple regions)
# - Bucket Storage class: standard



# Specify the required providers. This ensures that Terraform knows which provider (in this case, Google) and version to use,
# improving compatibility and reducing issues with version mismatches.
terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 4.0"  # Use the latest compatible version in the 4.x series
    }
  }

  required_version = ">= 1.0.0"  # Specify minimum Terraform version
}

# Configure the Google Cloud provider
provider "google" {
  project = var.GOOGLE_CLOUD_PROJECT_ID  # User's project name
  region  = var.GOOGLE_CLOUD_REGION
  zone    = var.GOOGLE_CLOUD_ZONE
}

# Define the Google Compute Engine instance (existing configuration)

resource "google_compute_instance" "de_zoomcamp_vm" {
  name         = var.GOOGLE_COMPUTE_INSTANCE_NAME
  machine_type = var.GOOGLE_COMPUTE_MACHINE_TYPE

  boot_disk {
    initialize_params {
      image = var.GOOGLE_CLOUD_BOOT_DISK_IMAGE
      size  = var.GOOGLE_CLOUD_BOOT_DISK_SIZE
      type  = var.GOOGLE_CLOUD_BOOT_DISK_TYPE
    }
  }


  metadata = {
    disable-legacy-endpoints = "true"
  }

  tags = ["de-zoomcamp", "via-terraform"]

  network_interface {
    network = "default"
    access_config {}
  }

  metadata_startup_script = <<-EOT
    #!/bin/bash
    echo "Hello, your VM is up and running!"
  EOT
}

# Firewall rule to allow SSH access
resource "google_compute_firewall" "allow_ssh" {
  name    = "allow-ssh"
  network = "default"

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["de-zoomcamp"]
}

# Google Cloud Storage bucket
resource "google_storage_bucket" "de_zoomcamp_bucket" {
  name          = var.GOOGLE_CLOUD_STORAGE_BUCKET_NAME
  location      = var.GOOGLE_CLOUD_STORAGE_BUCKET_LOCATION        # Multi-region location
  storage_class = var.GOOGLE_CLOUD_STORAGE_BUCKET_CLASS           # Standard storage class

  versioning {
    enabled = true  # Optional: enables versioning for objects in the bucket
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 365  # Optional: deletes objects older than 365 days
    }
  }
}
