terraform {
  backend "s3" {
    bucket       = "nz-electricity-tf-state"
    key          = "terraform.tfstate"
    region       = "ap-southeast-2"
    encrypt      = true
    use_lockfile = true
  }
}
