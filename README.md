# Ingesting Data from Splunk to Azure Data Explorer

The project provides modules to integrate Azure Data Explorer (on Azure and Microsoft Fabric) with Splunk

a) Export data continuously from Splunk index to ADX using Azure Data Explorer Splunk Addon
b) Export data to ADX as a target using Splunk universal forwarder
c) A sample Spark job to extract existing data from Splunk indexes to ADX for historical analysis

## Using Splunk Addon to ingest data from Splunk index to Azure Data Explorer
Prerequisites
Before getting started, ensure you have the following prerequisites in place:

1. A Splunk instance with the required privileges to install and configure add-ons.
2. Access to an Azure Data Explorer cluster.

### Step 1: Install the Splunk Addon for Azure Data Explorer
1. Download the Splunk Addon for Azure Data Explorer from the Splunkbase website.
2. Log in to your Splunk instance as an administrator.
3. Navigate to "Apps" and click on "Manage Apps."
4. Click on "Install app from file" and select the downloaded Splunk Addon for Azure Data Explorer file.
5. Follow the prompts to complete the installation

### Step 2: Create Splunk Index
1. Log in to your Splunk instance.
2. Navigate to "Settings" and click on "Indexes."
3. Click on "New Index" to create a new index.
4. Provide a name for the index and configure the necessary settings (e.g., retention period, data model, etc.).
5. Save the index configuration.

### Step 3: Configure Splunk Addon for Azure Data Explorer
1. In Splunk dashboard, Enter your search query in the Search bar based on which alerts will be generated and this alert data will be ingested to Azure Data Explorer.
3. Click on Save As and select Alert.
4. Provide a name for the alert and provide the interval at which the alert should be triggered.
5. Select the alert action as "Send to Azure Data Explorer."
6. Configure the Azure Data Explorer connection details such as application client Id, application client secret, cluster name, database name, table name.
7. Click on Save to save the alert configuration.

### Step 4: Verify the data in Azure Data Explorer
1. Start monitoring the Azure Data Explorer logs to ensure proper data ingestion.
2. Once the alert is triggered in Splunk, the data will be ingested to Azure Data Explorer.
3. Verify the data in Azure Data Explorer using the database and table name in the previous step.


## Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.opensource.microsoft.com.

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

## Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft 
trademarks or logos is subject to and must follow 
[Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/en-us/legal/intellectualproperty/trademarks/usage/general).
Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship.
Any use of third-party trademarks or logos are subject to those third-party's policies.
