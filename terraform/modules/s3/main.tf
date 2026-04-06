resource "aws_s3_bucket" "raw" {
  bucket = "${var.project_name}-raw-${var.name_suffix}"

  tags = {
    Project = var.project_name
    Purpose = "Raw and processed data storage"
  }
}

resource "aws_s3_bucket" "tfstate" {
  bucket = "${var.project_name}-tfstate-${var.name_suffix}"

  tags = {
    Project = var.project_name
    Purpose = "Terraform remote state"
  }
}

resource "aws_s3_bucket_versioning" "tfstate_versioning" {
  bucket = aws_s3_bucket.tfstate.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_dynamodb_table" "tf_locks" {
  name         = "${var.project_name}-tf-locks"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "LockID"

  attribute {
    name = "LockID"
    type = "S"
  }

  tags = {
    Project = var.project_name
    Purpose = "Terraform state locking"
  }
}