variable "project_name" {
  default = "youtube-analytics"
}

variable "environment" {
  default = "dev"
}

variable "aws_region" {
  default = "us-east-1"
}

variable "youtube_api_key" {
  description = "YouTube Data API v3 Key"
  sensitive   = true
}