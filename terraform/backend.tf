terraform {
  backend "s3" {
    bucket       = "tradesphere-tfstate-feyisayo"
    key          = "terraform.tfstate"
    region       = "us-east-1"
    use_lockfile = true
    encrypt      = true
  }
}