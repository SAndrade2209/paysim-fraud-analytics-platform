###############################################################################
# Variables — Snowflake module
###############################################################################

variable "snowflake_org" {
  description = "Snowflake organizaroin that Terraform authenticates as (must have SYSADMIN)"
  type        = string
}

variable "snowflake_password"{
    description = "Password for the Snowflake user (if not using key-pair authentication)"
     type        = string
    default     = ""
}

variable "snowflake_account" {
  description = "Snowflake account locator"
  type        = string
}

variable "snowflake_user" {
  description = "Snowflake user that Terraform authenticates as (must have SYSADMIN)"
  type        = string
}

variable "snowflake_private_key_path" {
  description = "Path to the RSA private key (.p8) used for key-pair authentication"
  type        = string
  default     = "../../../secrets/snowflake_key.p8"
}
