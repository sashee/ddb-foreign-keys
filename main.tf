provider "aws" {
}

# DDB

resource "random_id" "id" {
  byte_length = 8
}

resource "aws_dynamodb_table" "group" {
  name         = "group-${random_id.id.hex}"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "ID"

  attribute {
    name = "ID"
    type = "S"
  }
}
resource "aws_dynamodb_table" "user" {
  name         = "user-${random_id.id.hex}"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "ID"

  attribute {
    name = "ID"
    type = "S"
  }
}

output "group-table" {
	value = aws_dynamodb_table.group.id
}
output "user-table" {
	value = aws_dynamodb_table.user.id
}
