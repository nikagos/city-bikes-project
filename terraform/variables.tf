variable "GOOGLE_CLOUD_PROJECT_ID" {
  description = "ID of your google cloud project"
  type = string
  default = "steady-webbing-436216-k1"
}

variable "GOOGLE_CLOUD_REGION" {
  description = "Region that your google cloud project is hosted in"
  type = string
  default = "northamerica-northeast1"
}

variable "GOOGLE_CLOUD_ZONE" {
  description = "Zone that your google cloud project is hosted in"
  type = string
  default = "northamerica-northeast1-b"
}

variable "GOOGLE_COMPUTE_INSTANCE_NAME" {
  description = "Name of your cloud VM environment"
  type = string
  default = "de-zoomcamp-via-terraform"
}

variable "GOOGLE_COMPUTE_MACHINE_TYPE" {
  description = "Machine type of your cloud VM environment"
  type = string
  default = "e2-standard-4"
}

variable "GOOGLE_CLOUD_STORAGE_BUCKET_NAME" {
  description = "Bucket name you want to add csv file to"
  type = string
  default = "de-zoomcamp-bucket-via-terraform"
}

variable "GOOGLE_CLOUD_STORAGE_BUCKET_LOCATION" {
  description = "Location of then bucket name you want to add csv file to"
  type = string
  default = "de-zoomcamp-bucket-via-terraform"
}

variable "GOOGLE_CLOUD_STORAGE_BUCKET_CLASS" {
  description = "Class of your GCS bucket"
  type = string
  default = "STANDARD"
}

variable "GOOGLE_CLOUD_BOOT_DISK_IMAGE" {
  description = "Image of the boot disk"
  type = string
  default = "ubuntu-os-cloud/ubuntu-2004-lts"
}

variable "GOOGLE_CLOUD_BOOT_DISK_SIZE" {
  description = "Size of the boot disk"
  type = number
  default = 50
}

variable "GOOGLE_CLOUD_BOOT_DISK_TYPE" {
  description = "Type of the boot disk"
  type = string
  default = "pd-balanced"
}