# Ingesting Data from Splunk to Azure Data Explorer using Azure Data Explorer Addon

Microsoft Azure Data Explorer Add-On for Splunk allows Azure Data Explorer users to ingest logs from splunk platform using the kusto python sdk.
Learn More about the Azure Data Explorer at https://learn.microsoft.com/en-us/azure/data-explorer/

## Details
Details on pre-requisites, configuring the add-on and viewing the data in Azure Data Explorer is covered in this section.

### Background
When we add data to Splunk, the Splunk indexer processes it and stores it in a designated index (either, by default, in the main index or custom index). Searching in Splunk involves using the indexed data for the purpose of creating metrics, dashboards and alerts. This Splunk add-on triggers an action based on the alert in Splunk. We can use Alert actions to send data to Azure Data Explorer using the specified addon.

This add-on uses kusto python sdk(https://learn.microsoft.com/en-us/azure/data-explorer/kusto/api/python/kusto-python-client-library)  to send log data to Microsoft Azure Data Explorer. Hence, this addon supports queued mode of ingestion by default. This addon has a durable feature as well which helps to minimize data loss during any unexpected network error scenarios. But durability in ingestion comes at the cost of throughput, so it is advised to use this option judiciously.

### Prerequisites
1. A Splunk instance (latest release) with the required installation privileges to configure add-ons.
2. Access to an Azure Data Explorer cluster.

### Step 1: Install the Azure Data Explorer Addon
1. Download the Azure Data Explorer Addon from the Splunkbase website.
2. Log in to your Splunk instance as an administrator.
3. Navigate to "Apps" and click on "Manage Apps."
4. Click on "Install app from file" and select the downloaded Splunk Addon for Azure Data Explorer file.
5. Follow the prompts to complete the installation

After installation of the Splunk Addon for alerts, it should be visible in the Dashboard -> Alert Actions 
![Alt text](https://github.com/Azure/azure-kusto-splunk/blob/main/splunk-adx-alert-addon/resources/ADX_Addon_created_addon.png "ADX Addon Created")

### Step 2: Create Splunk Index
1. Log in to your Splunk instance.
2. Navigate to "Settings" and click on "Indexes."
3. Click on "New Index" to create a new index.
4. Provide a name for the index and configure the necessary settings (e.g., retention period, data model, etc.).
5. Save the index configuration.

### Step 3: Configure Splunk Addon for Azure Data Explorer
1. In Splunk dashboard, Enter your search query in the Search bar based on which alerts will be generated and this alert data will be ingested to Azure Data Explorer.
2. Click on Save As and select Alert.
3. Provide a name for the alert and provide the interval at which the alert should be triggered.
4. Select the alert action as "Send to Azure Data Explorer."
   ![Alt text](https://github.com/Azure/azure-kusto-splunk/blob/main/splunk-adx-alert-addon/resources/ADX_Addon_Alert_Trigger.png "ADX Addon Alert Trigger")
5. Configure the Azure Data Explorer connection details such as application client Id, application client secret, cluster name, database name, table name.
   ![Alt text](https://github.com/Azure/azure-kusto-splunk/blob/main/splunk-adx-alert-addon/resources/ADX_Addon_Alert_desc.png "ADX Addon Alert Desc")
6. When the alert is created, it should be visible in Splunk Dashboard -> Alerts
   ![Alt text](https://github.com/Azure/azure-kusto-splunk/blob/main/splunk-adx-alert-addon/resources/ADX_Addon_saved_alert.png "ADX Addon Alert Desc")

### Step 4: Verify the data in Azure Data Explorer
1. Start monitoring the Azure Data Explorer logs to ensure proper data ingestion.
2. Once the alert is triggered in Splunk, the data will be ingested to Azure Data Explorer.
3. Verify the data in Azure Data Explorer using the database and table name in the previous step.
   ![Alt text](https://github.com/Azure/azure-kusto-splunk/blob/main/splunk-adx-alert-addon/resources/ADX_table_data.png "ADX table")

## Azure Data Explorer Addon Parameters
The following is the list of parameters which need to be entered/selected while configuring the addon
1. Azure Cluster Ingestion URL: Represents the ingestion URL of the Azure Data Explorer cluster in the ADX portal.
2. Azure Application Client Id: Represents the Azure Application Client Id credentials required to access the Azure Data Explorer cluster.
3. Azure Application Client secret: Represents the Azure Application Client secret credentials required to access the Azure Data Explorer cluster.
4. Azure Application Tenant Id: Represents the Azure Application Tenant Id required to access the Azure Data Explorer cluster.
5. ADX Database Name: This represents the name of the database created in the Azure Data Explorer cluster, where we want our data to be ingested.
6. ADX Table Name: This represents the name of the table inside the database created in the Azure Data Explorer cluster, where we want our data to be ingested.
7. ADX Table Mapping Name: This represents the Azure Data Explorer table mapping used to map to the column of created ADX table.
8. Remove Extra Fields : This represents whether we want to remove empty fields in the splunk event payload
9. Durable Mode : This property specifies whether durability mode is required during ingestion. When set to true, the ingestion throughput is impacted.


