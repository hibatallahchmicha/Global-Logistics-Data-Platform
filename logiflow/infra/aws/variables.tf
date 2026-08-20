variable "aws_region" {
  description = "AWS region for all LogiFlow resources"
  type        = string
  default     = "eu-north-1"
}

variable "project_name" {
  description = "Prefix applied to every resource name"
  type        = string
  default     = "logiflow"
}