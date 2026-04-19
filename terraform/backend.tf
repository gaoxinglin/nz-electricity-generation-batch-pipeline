terraform {
  backend "s3" {
    bucket         = "nz-electricity-tf-state"
    key            = "terraform.tfstate"
    region         = "ap-southeast-2"
    encrypt        = true
    dynamodb_table = "nz-electricity-tf-lock"
  }
}
