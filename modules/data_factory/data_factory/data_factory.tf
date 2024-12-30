# Create the Azure Data Factory Resource
resource "azurerm_data_factory" "cicdADF" {
  location            = var.location
  name                = var.adf_name
  resource_group_name = var.resource_group_name
  tags                     = {
    environment = "development"
  }
  identity {
    type = "SystemAssigned"
  }
}

# Data instance
data "azurerm_storage_account" "source_folder_storage" {
  name                = var.storage_account_name
  resource_group_name = var.resource_group_name
}

data "azurerm_storage_account" "destination_folder_storage" {
  name                = var.storage_account_name
  resource_group_name = var.resource_group_name
}

# Create the Azure Data Factory Linked Services of Azure blob Storage(Source + Destination)
resource "azurerm_data_factory_linked_service_azure_blob_storage" "source-linked-service" {
  name                = "source-storage-linked-service"
  data_factory_id     = azurerm_data_factory.cicdADF.id
  connection_string = data.azurerm_storage_account.source_folder_storage.primary_connection_string
}

resource "azurerm_data_factory_linked_service_azure_blob_storage" "destination-linked-service" {
  name                = "destination-storage-linked-service"
  data_factory_id     = azurerm_data_factory.cicdADF.id
  connection_string = data.azurerm_storage_account.destination_folder_storage.primary_connection_string
}

# Create the Azure Data factory Dataset
resource "azurerm_data_factory_dataset_binary" "source-dataset" {
  data_factory_id     = azurerm_data_factory.cicdADF.id
  linked_service_name = azurerm_data_factory_linked_service_azure_blob_storage.source-linked-service.name
  name                = "source_dataset"
  sftp_server_location {
    filename = "test.txt"
    path     = "source"
  }
}

resource "azurerm_data_factory_dataset_binary" "destination-dataset" {
  data_factory_id     = azurerm_data_factory.cicdADF.id
  linked_service_name = azurerm_data_factory_linked_service_azure_blob_storage.destination-linked-service.name
  name                = "destination_dataset"
  sftp_server_location {
    filename = "test-${formatdate("YYYY-MM-DD-hh-mm-ss", timestamp())}.txt"
    path     = "destination"
  }
}

# Create the Pipeline to copy the binary data file from Source blob storage to the Destination blob storage container
resource "azurerm_data_factory_pipeline" "copy_data_pipeline" {
  name                = "copy_binary_file_pipeline"
  data_factory_id     = azurerm_data_factory.cicdADF.id
  activities_json = <<JSON
[
  {
    "name": "CopyBinaryFileFromSourceToDestination",
    "type": "Copy",
    "typeProperties": {
      "source": {
        "type": "BinarySource",
        "recursive": true
      },
      "sink": {
        "type": "BinarySink"
      },
      "enableStaging": false
    },
    "policy": {
      "timeout": "7.00:00:00",
      "retry": 0,
      "retryIntervalInSeconds": 30,
      "secureInput": false,
      "secureOutput": false
    },
    "scheduler": {
      "frequency": "Day",
      "interval": 1
    },
    "external": true,
     "inputs": [
      {
        "referenceName": "source_dataset",
        "type": "DatasetReference"
      }
    ],
    "outputs": [
      {
        "referenceName": "destination_dataset",
        "type": "DatasetReference"
      }
    ]
  }
]
JSON
  depends_on = [
    azurerm_data_factory_dataset_binary.source-dataset,
    azurerm_data_factory_dataset_binary.destination-dataset
  ]
}