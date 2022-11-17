# Backend configuration require a AWS storage bucket.
terraform {
  backend "s3" {
    bucket = "terraform-state-tracks"
    key    = "state/tracks/terraform.tfstate"
    region = "us-east-2"
  }
}