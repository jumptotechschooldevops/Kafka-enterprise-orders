terraform {
  backend "s3" {
    bucket         = "aisalkyn-kafka-tfstate"
    key            = "terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "aisalkyn-terraform-lock"
    encrypt        = true
  }
}
