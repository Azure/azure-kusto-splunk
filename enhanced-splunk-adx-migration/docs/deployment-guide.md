# Enhanced Splunk to ADX Migration - Deployment Guide

## Overview

This guide provides step-by-step instructions for deploying and running the enhanced Splunk to Azure Data Explorer (ADX) migration solution. The solution is optimized for large-scale data transfers (250TB+) using Azure Databricks.

## Architecture

```
┌─────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Splunk    │───▶│ Azure Databricks │───▶│ Azure Data      │
│   Cluster   │    │ + Spark          │    │ Explorer (ADX)  │
└─────────────┘    └──────────────────┘    └─────────────────┘
                            │
                   ┌─────────────────────┐
                   │ Progress Dashboard  │
                   │ & Monitoring        │
                   └─────────────────────┘
```

## Prerequisites

### Azure Resources
1. **Azure Databricks Workspace** - Premium tier recommended
2. **Azure Data Explorer Cluster** - Optimized compute SKU
3. **Azure Active Directory App Registration** - For authentication
4. **Storage Account** - For temporary staging (optional)

### Access Requirements
- Databricks workspace administrator access
- ADX database contributor/admin permissions
- Network connectivity between Databricks and Splunk
- Network connectivity between Databricks and ADX

### Local Development Environment
- Python 3.8+ 
- Git
- Azure CLI (optional, for authentication)

## Step 1: Environment Setup

### 1.1 Clone and Prepare Files
```bash
# Clone the repository (or copy the enhanced migration files)
git clone <repository-url>
cd enhanced-splunk-adx-migration

# Install local dependencies (for dashboard and utilities)
pip install -r requirements.txt
```

### 1.2 Configure Migration Settings
Edit `config/migration-config.yaml`:

```yaml
# Update these critical settings
migration:
  indexes: ["your-index-1", "your-index-2"]  # Your actual Splunk indexes
  start_date: "2020-01-01"                   # Adjust date range
  end_date: "2025-01-01"

splunk:
  host: "your-splunk-host.com"               # Your Splunk server
  username: "your-username"
  password: "your-password"

adx:
  cluster: "https://your-cluster.kusto.windows.net"
  database: "YourDatabase"
  table: "YourTable"
  client_id: "your-client-id"
  client_secret: "your-client-secret"
  authority: "your-tenant-id"
```

### 1.3 Create Azure AD Application
```bash
# Using Azure CLI
az ad app create --display-name "Splunk-ADX-Migration"
az ad sp create --id <app-id>
az ad sp credential reset --id <app-id>

# Note down:
# - Application (client) ID
# - Client Secret
# - Tenant ID
```

## Step 2: Azure Data Explorer Setup

### 2.1 Create Database and Table
```kusto
// Connect to your ADX cluster and run these commands

// Create database
.create database SplunkData

// Use the database
.use database SplunkData

// Create table with flexible schema
.create table Events (
    timestamp: datetime,
    raw_data: string,
    source: string,
    sourcetype: string,
    host: string,
    index_name: string,
    migration_timestamp: datetime,
    partition_id: string
)

// Create ingestion mapping
.create table Events ingestion json mapping "Events_mapping"
'['
'    {"column": "timestamp", "path": "$._time", "transform": "DateTimeFromUnixSeconds"},'
'    {"column": "raw_data", "path": "$._raw"},'
'    {"column": "source", "path": "$.source"},'
'    {"column": "sourcetype", "path": "$.sourcetype"},'
'    {"column": "host", "path": "$.host"},'
'    {"column": "index_name", "path": "$.index"},'
'    {"column": "migration_timestamp", "path": "$.migration_timestamp"},'
'    {"column": "partition_id", "path": "$.partition_id"}'
']'
```

### 2.2 Grant Permissions
```kusto
// Grant permissions to the service principal
.add database SplunkData admins ('aadapp=<client-id>;<tenant-id>')
```

## Step 3: Azure Databricks Setup

### 3.1 Manual Setup (Option A)

#### Create Databricks Workspace
1. Go to Azure Portal → Create Resource → Azure Databricks
2. Choose Premium tier for advanced features
3. Configure networking and security as needed

#### Create Cluster
1. Go to Databricks workspace → Compute → Create Cluster
2. Use these recommended settings:
   - **Cluster Mode**: Standard
   - **Databricks Runtime**: 13.3 LTS (latest stable)
   - **Node Type**: Standard_DS3_v2 (4 cores, 14GB RAM)
   - **Workers**: 10-50 (adjust based on data volume)
   - **Auto Scaling**: Enabled

#### Configure Cluster
Add these Spark configurations in Advanced Options:
```
spark.sql.adaptive.enabled true
spark.sql.adaptive.coalescePartitions.enabled true
spark.serializer org.apache.spark.serializer.KryoSerializer
spark.sql.execution.arrow.pyspark.enabled true
spark.sql.files.maxPartitionBytes 134217728
spark.sql.shuffle.partitions 800
```

#### Install Libraries
Install these libraries on the cluster:
- `azure-kusto-data>=4.0.0`
- `azure-kusto-ingest>=4.0.0`
- `splunk-sdk>=1.7.3`
- `PyYAML>=6.0`
- Maven: `com.microsoft.azure.kusto:kusto-spark_3.0_2.12:3.1.15`

### 3.2 Automated Setup (Option B - Recommended)

Use the provided setup script:
```bash
python deployment/databricks_setup.py \
    --workspace-url "https://adb-<workspace-id>.<random-number>.azuredatabricks.net" \
    --access-token "<your-databricks-token>" \
    --config-path "config/migration-config.yaml" \
    --cluster-name "splunk-migration-cluster" \
    --job-name "splunk-adx-migration-job" \
    --local-files-path "." \
    --action "setup"
```

### 3.3 Generate Databricks Access Token
1. Go to Databricks workspace → User Settings → Access Tokens
2. Generate new token with appropriate expiration
3. Store securely for API access

## Step 4: Upload Migration Files

### 4.1 Manual Upload
1. Go to Databricks workspace → Data → DBFS File Browser
2. Create folder: `/migration/`
3. Upload files:
   - `enhanced_migration.py`
   - `config/migration-config.yaml`
   - `monitoring/progress_dashboard.py`

### 4.2 Automated Upload
The setup script handles this automatically, or use CLI:
```bash
databricks fs cp enhanced_migration.py dbfs:/migration/
databricks fs cp config/migration-config.yaml dbfs:/migration/config/
```

## Step 5: Running the Migration

### 5.1 Start Migration Job
```python
# Using the setup script
python deployment/databricks_setup.py \
    --workspace-url "<workspace-url>" \
    --access-token "<token>" \
    --config-path "config/migration-config.yaml" \
    --action "start" \
    --job-id "<job-id>"
```

### 5.2 Manual Execution
1. Create Databricks notebook
2. Install libraries on cluster
3. Copy migration script content
4. Run with your configuration

### 5.3 Command Line Execution
```bash
# On Databricks cluster
python enhanced_migration.py --config config/migration-config.yaml
```

## Step 6: Monitoring and Management

### 6.1 Real-time Dashboard
```bash
# Start monitoring dashboard (local)
streamlit run monitoring/progress_dashboard.py

# Or deploy to cloud for team access
```

### 6.2 Databricks Monitoring
- Monitor cluster metrics in Databricks UI
- Check job logs and execution status
- Set up alerts for failures

### 6.3 ADX Monitoring
```kusto
// Check ingestion status
.show ingestion failures
| where FailedOn > ago(1d)

// Monitor data volume
Events
| summarize count() by bin(timestamp, 1h)
| render timechart

// Check partition progress
Events
| distinct partition_id
| count
```

## Step 7: Optimization and Scaling

### 7.1 Performance Tuning
- **Cluster Scaling**: Increase workers for faster processing
- **Partition Size**: Adjust based on data density
- **Parallelism**: Tune concurrent partition processing
- **Memory**: Monitor and adjust executor memory

### 7.2 Cost Optimization
- **Auto-scaling**: Use auto-scaling clusters
- **Spot Instances**: Consider for non-critical workloads
- **Scheduling**: Run during off-peak hours
- **Monitoring**: Track DBU consumption

### 7.3 Troubleshooting Common Issues

#### Connection Issues
```bash
# Test Splunk connectivity
python -c "import splunklib.client as client; service = client.connect(host='your-host', port=8089, username='user', password='pass')"

# Test ADX connectivity
python -c "from azure.kusto.data import KustoClient, KustoConnectionStringBuilder; kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication('cluster', 'client_id', 'secret', 'tenant')"
```

#### Memory Issues
- Increase executor memory in cluster configuration
- Reduce batch sizes in configuration
- Enable memory optimization features

#### Network Timeouts
- Increase timeout values in configuration
- Check network connectivity and firewall rules
- Consider using VNet peering for better connectivity

## Step 8: Security Considerations

### 8.1 Authentication
- Use Azure AD service principals
- Rotate secrets regularly
- Use Azure Key Vault for secret management

### 8.2 Network Security
- Configure VNet integration
- Use private endpoints
- Implement proper firewall rules

### 8.3 Data Security
- Enable encryption in transit and at rest
- Implement proper access controls
- Monitor data access patterns

## Step 9: Migration Completion

### 9.1 Data Validation
```kusto
// Validate row counts
let splunk_count = <expected_row_count>;
let adx_count = Events | count;
print splunk_count, adx_count, difference = splunk_count - adx_count
```

### 9.2 Cleanup
- Terminate Databricks clusters
- Clean up temporary files
- Archive migration logs

## Resource Estimation for 250TB

### Recommended Configuration
- **Cluster Size**: 20-50 worker nodes
- **Runtime**: 2-3 weeks
- **Cost**: ~$15,000-30,000 (estimated)
- **Network**: High bandwidth connection required

### Timeline Breakdown
- Setup: 1-2 days
- Testing: 2-3 days
- Migration: 14-21 days
- Validation: 1-2 days

## Support and Troubleshooting

### Logs and Diagnostics
- Databricks job logs
- Migration application logs in `/dbfs/migration-logs/`
- ADX ingestion logs
- System performance metrics

### Common Solutions
1. **Slow Performance**: Increase cluster size, optimize partition sizes
2. **Connection Timeouts**: Increase timeout values, check network
3. **Memory Errors**: Increase executor memory, reduce batch sizes
4. **Authentication Failures**: Verify credentials and permissions

### Getting Help
- Check logs for detailed error messages
- Monitor system resources and performance
- Use the monitoring dashboard for real-time insights
- Contact Azure support for platform-specific issues

---

## Quick Start Checklist

- [ ] Configure migration settings in `config/migration-config.yaml`
- [ ] Create Azure AD application and note credentials
- [ ] Set up ADX database and table with proper permissions
- [ ] Create Databricks workspace and cluster
- [ ] Upload migration files to Databricks
- [ ] Install required libraries
- [ ] Start migration job
- [ ] Monitor progress using dashboard
- [ ] Validate data after completion

This deployment guide provides a comprehensive approach to setting up and running the enhanced Splunk to ADX migration. Follow the steps carefully and monitor the process closely for optimal results.
