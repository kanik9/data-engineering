variable "subscription_id"{
  description = ""
  type= string
}

variable "resource_group_name" {
  description = "The name of the resource group"
  type = string
}

variable "location" {
  description = "The location/region of the resource group"
  type = string
}

variable "tags" {
  description = "The tags associated of the resource group"
  type = map(string)
}

variable "name" {
  description = "The location/region of the resource group"
  type = string
}

variable "destination_folder_name" {
  description = "The location/region of the resource group"
  type = string
}

variable "source_folder_name" {
  description = "The location/region of the resource group"
  type = string
}