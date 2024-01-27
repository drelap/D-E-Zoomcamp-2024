variable "project" {
  description = "Project"
  default     = "polished-engine-412507"
}

variable "region" {
  description = "Region"
  #Update the below to your desired region
  default     = "asia-southeast2"
}

variable "location" {
  description = "Project Location"
  #Update the below to your desired location
  default     = "ASIA"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  #Update the below to what you want your dataset to be called
  default     = "terraform_demo_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  #Update the below to a unique bucket name
  default     = "polished-engine-412507-terra-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}