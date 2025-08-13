"""
Azure Databricks Setup and Deployment Script
Automates the setup of Databricks cluster and environment for Splunk to ADX migration
"""

import json
import time
import requests
import yaml
from typing import Dict, List, Optional
import logging
from pathlib import Path
import os

class DatabricksSetup:
    def __init__(self, workspace_url: str, access_token: str, config_path: str):
        self.workspace_url = workspace_url.rstrip('/')
        self.access_token = access_token
        self.headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.logger = self._setup_logging()
    
    def _setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('databricks_setup.log'),
                logging.StreamHandler()
            ]
        )
        return logging.getLogger(__name__)
    
    def create_cluster(self, cluster_name: str) -> str:
        """Create optimized Databricks cluster for large-scale migration."""
        
        cluster_config = {
            "cluster_name": cluster_name,
            "spark_version": "13.3.x-scala2.12",  # Latest stable version
            "node_type_id": "Standard_DS3_v2",    # 4 cores, 14GB RAM
            "driver_node_type_id": "Standard_DS4_v2",  # 8 cores, 28GB RAM for driver
            "num_workers": self.config['resource_estimation']['recommended_cluster']['min_workers'],
            "autoscale": {
                "min_workers": self.config['resource_estimation']['recommended_cluster']['min_workers'],
                "max_workers": self.config['resource_estimation']['recommended_cluster']['max_workers']
            },
            "auto_termination_minutes": 120,  # Auto-terminate after 2 hours of inactivity
            "spark_conf": {
                # Optimize for large-scale data processing
                "spark.databricks.delta.preview.enabled": "true",
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                "spark.sql.execution.arrow.pyspark.enabled": "true",
                "spark.databricks.cluster.profile": "serverless",
                "spark.databricks.passthrough.enabled": "true",
                
                # Memory optimization for 250TB processing
                "spark.sql.execution.arrow.maxRecordsPerBatch": "10000",
                "spark.sql.files.maxPartitionBytes": "134217728",  # 128MB partitions
                "spark.sql.shuffle.partitions": "800",  # Optimize for large datasets
                
                # Network and I/O optimization
                "spark.network.timeout": "800s",
                "spark.sql.broadcastTimeout": "36000",
                "spark.rpc.askTimeout": "300s",
                "spark.rpc.lookupTimeout": "300s",
                
                # Kusto connector optimization
                "spark.kusto.write.batchLimit": "1000",
                "spark.kusto.write.timeout": "10m"
            },
            "custom_tags": {
                "Project": "Splunk-ADX-Migration",
                "Purpose": "250TB-Data-Transfer",
                "Environment": "Production"
            },
            "init_scripts": [
                {
                    "dbfs": {
                        "destination": "dbfs:/databricks/scripts/install-kusto-connector.sh"
                    }
                }
            ],
            "libraries": [
                {"pypi": {"package": "azure-kusto-data>=4.0.0"}},
                {"pypi": {"package": "azure-kusto-ingest>=4.0.0"}},
                {"pypi": {"package": "splunk-sdk>=1.7.3"}},
                {"pypi": {"package": "PyYAML>=6.0"}},
                {"pypi": {"package": "pandas>=2.0.0"}},
                {"pypi": {"package": "pyarrow>=12.0.0"}},
                {"maven": {"coordinates": "com.microsoft.azure.kusto:kusto-spark_3.0_2.12:3.1.15"}}
            ]
        }
        
        response = requests.post(
            f"{self.workspace_url}/api/2.0/clusters/create",
            headers=self.headers,
            data=json.dumps(cluster_config)
        )
        
        if response.status_code == 200:
            cluster_id = response.json()['cluster_id']
            self.logger.info(f"Cluster created successfully: {cluster_id}")
            self._wait_for_cluster_ready(cluster_id)
            return cluster_id
        else:
            self.logger.error(f"Failed to create cluster: {response.text}")
            raise Exception(f"Cluster creation failed: {response.text}")
    
    def _wait_for_cluster_ready(self, cluster_id: str, timeout: int = 1800):
        """Wait for cluster to be ready (up to 30 minutes)."""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            response = requests.get(
                f"{self.workspace_url}/api/2.0/clusters/get",
                headers=self.headers,
                params={"cluster_id": cluster_id}
            )
            
            if response.status_code == 200:
                state = response.json()['state']
                self.logger.info(f"Cluster state: {state}")
                
                if state == 'RUNNING':
                    self.logger.info("Cluster is ready!")
                    return
                elif state in ['TERMINATED', 'ERROR']:
                    raise Exception(f"Cluster failed to start. State: {state}")
            
            time.sleep(30)  # Check every 30 seconds
        
        raise Exception("Cluster startup timeout")
    
    def create_init_script(self):
        """Create initialization script for Kusto connector setup."""
        init_script_content = """#!/bin/bash
# Databricks initialization script for Kusto Spark connector

echo "Starting Kusto connector setup..."

# Download and install Kusto Spark connector
mkdir -p /databricks/jars
cd /databricks/jars

# Download the latest Kusto Spark connector
wget -O kusto-spark.jar "https://repo1.maven.org/maven2/com/microsoft/azure/kusto/kusto-spark_3.0_2.12/3.1.15/kusto-spark_3.0_2.12-3.1.15-jar-with-dependencies.jar"

# Set permissions
chmod 644 kusto-spark.jar

echo "Kusto connector installed successfully"

# Install additional system packages if needed
sudo apt-get update
sudo apt-get install -y htop iotop

echo "System packages installed"

# Configure logging
mkdir -p /dbfs/migration-logs
chmod 777 /dbfs/migration-logs

echo "Migration environment setup completed"
"""
        
        # Upload init script to DBFS
        self._upload_to_dbfs(
            "dbfs:/databricks/scripts/install-kusto-connector.sh",
            init_script_content
        )
        
        self.logger.info("Initialization script created and uploaded")
    
    def _upload_to_dbfs(self, path: str, content: str):
        """Upload content to DBFS."""
        import base64
        
        # Create DBFS directory
        dir_path = "/".join(path.split("/")[:-1])
        requests.post(
            f"{self.workspace_url}/api/2.0/dbfs/mkdirs",
            headers=self.headers,
            data=json.dumps({"path": dir_path})
        )
        
        # Upload file
        encoded_content = base64.b64encode(content.encode()).decode()
        response = requests.post(
            f"{self.workspace_url}/api/2.0/dbfs/put",
            headers=self.headers,
            data=json.dumps({
                "path": path,
                "contents": encoded_content,
                "overwrite": True
            })
        )
        
        if response.status_code != 200:
            raise Exception(f"Failed to upload to DBFS: {response.text}")
    
    def upload_migration_files(self, local_path: str, dbfs_path: str = "/dbfs/migration"):
        """Upload migration files to DBFS."""
        files_to_upload = [
            "enhanced_migration.py",
            "config/migration-config.yaml",
            "monitoring/progress_dashboard.py",
            "requirements.txt"
        ]
        
        for file_path in files_to_upload:
            full_local_path = os.path.join(local_path, file_path)
            if os.path.exists(full_local_path):
                with open(full_local_path, 'r') as f:
                    content = f.read()
                
                dbfs_file_path = f"{dbfs_path}/{file_path}"
                self._upload_to_dbfs(dbfs_file_path, content)
                self.logger.info(f"Uploaded {file_path} to {dbfs_file_path}")
    
    def create_job(self, cluster_id: str, job_name: str) -> str:
        """Create Databricks job for migration."""
        
        job_config = {
            "name": job_name,
            "new_cluster": {
                "cluster_id": cluster_id
            },
            "spark_python_task": {
                "python_file": "dbfs:/migration/enhanced_migration.py",
                "parameters": ["--config", "dbfs:/migration/config/migration-config.yaml"]
            },
            "libraries": [
                {"pypi": {"package": "azure-kusto-data>=4.0.0"}},
                {"pypi": {"package": "azure-kusto-ingest>=4.0.0"}},
                {"pypi": {"package": "splunk-sdk>=1.7.3"}},
                {"pypi": {"package": "PyYAML>=6.0"}},
                {"maven": {"coordinates": "com.microsoft.azure.kusto:kusto-spark_3.0_2.12:3.1.15"}}
            ],
            "timeout_seconds": 86400,  # 24 hours timeout
            "max_retries": 3,
            "retry_on_timeout": True,
            "email_notifications": {
                "on_start": [],
                "on_success": [],
                "on_failure": []
            },
            "webhook_notifications": {},
            "tags": {
                "Project": "Splunk-ADX-Migration",
                "Type": "Data-Transfer-Job"
            }
        }
        
        response = requests.post(
            f"{self.workspace_url}/api/2.1/jobs/create",
            headers=self.headers,
            data=json.dumps(job_config)
        )
        
        if response.status_code == 200:
            job_id = response.json()['job_id']
            self.logger.info(f"Job created successfully: {job_id}")
            return job_id
        else:
            self.logger.error(f"Failed to create job: {response.text}")
            raise Exception(f"Job creation failed: {response.text}")
    
    def start_job(self, job_id: str) -> str:
        """Start the migration job."""
        response = requests.post(
            f"{self.workspace_url}/api/2.1/jobs/run-now",
            headers=self.headers,
            data=json.dumps({"job_id": job_id})
        )
        
        if response.status_code == 200:
            run_id = response.json()['run_id']
            self.logger.info(f"Job started successfully: {run_id}")
            return run_id
        else:
            self.logger.error(f"Failed to start job: {response.text}")
            raise Exception(f"Job start failed: {response.text}")
    
    def monitor_job(self, run_id: str):
        """Monitor job execution."""
        while True:
            response = requests.get(
                f"{self.workspace_url}/api/2.1/jobs/runs/get",
                headers=self.headers,
                params={"run_id": run_id}
            )
            
            if response.status_code == 200:
                job_info = response.json()
                state = job_info['state']['life_cycle_state']
                
                self.logger.info(f"Job state: {state}")
                
                if state == 'TERMINATED':
                    result_state = job_info['state']['result_state']
                    if result_state == 'SUCCESS':
                        self.logger.info("Job completed successfully!")
                        return True
                    else:
                        self.logger.error(f"Job failed with state: {result_state}")
                        return False
                elif state in ['INTERNAL_ERROR', 'FAILED', 'CANCELLED']:
                    self.logger.error(f"Job failed with state: {state}")
                    return False
            
            time.sleep(60)  # Check every minute
    
    def setup_complete_environment(self, cluster_name: str, job_name: str, local_files_path: str):
        """Complete environment setup workflow."""
        self.logger.info("Starting complete Databricks environment setup...")
        
        try:
            # Step 1: Create initialization script
            self.logger.info("Creating initialization script...")
            self.create_init_script()
            
            # Step 2: Create cluster
            self.logger.info(f"Creating cluster: {cluster_name}")
            cluster_id = self.create_cluster(cluster_name)
            
            # Step 3: Upload migration files
            self.logger.info("Uploading migration files...")
            self.upload_migration_files(local_files_path)
            
            # Step 4: Create job
            self.logger.info(f"Creating job: {job_name}")
            job_id = self.create_job(cluster_id, job_name)
            
            self.logger.info("Environment setup completed successfully!")
            self.logger.info(f"Cluster ID: {cluster_id}")
            self.logger.info(f"Job ID: {job_id}")
            
            return {
                "cluster_id": cluster_id,
                "job_id": job_id,
                "workspace_url": self.workspace_url
            }
            
        except Exception as e:
            self.logger.error(f"Environment setup failed: {e}")
            raise
    
    def get_cluster_cost_estimate(self, cluster_id: str, hours: int) -> Dict:
        """Estimate cluster costs."""
        # Get cluster configuration
        response = requests.get(
            f"{self.workspace_url}/api/2.0/clusters/get",
            headers=self.headers,
            params={"cluster_id": cluster_id}
        )
        
        if response.status_code == 200:
            cluster_info = response.json()
            num_workers = cluster_info.get('num_workers', 0)
            
            # Rough cost estimation (this would need to be updated with current Azure pricing)
            cost_per_dbu_hour = 0.40  # Approximate cost per DBU hour
            dbu_per_node = 2  # Standard_DS3_v2 typically uses 2 DBUs
            
            total_nodes = num_workers + 1  # Workers + driver
            total_dbu_hours = total_nodes * dbu_per_node * hours
            estimated_cost = total_dbu_hours * cost_per_dbu_hour
            
            return {
                "total_nodes": total_nodes,
                "dbu_per_hour": total_nodes * dbu_per_node,
                "total_dbu_hours": total_dbu_hours,
                "estimated_cost_usd": estimated_cost,
                "hours": hours
            }
        else:
            raise Exception("Failed to get cluster information")

def main():
    """Main function for command-line usage."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Azure Databricks Setup for Splunk Migration")
    parser.add_argument("--workspace-url", required=True, help="Databricks workspace URL")
    parser.add_argument("--access-token", required=True, help="Databricks access token")
    parser.add_argument("--config-path", required=True, help="Migration configuration file path")
    parser.add_argument("--cluster-name", default="splunk-migration-cluster", help="Cluster name")
    parser.add_argument("--job-name", default="splunk-adx-migration-job", help="Job name")
    parser.add_argument("--local-files-path", required=True, help="Local migration files path")
    parser.add_argument("--action", choices=["setup", "start", "monitor"], default="setup", help="Action to perform")
    parser.add_argument("--job-id", help="Job ID for start/monitor actions")
    parser.add_argument("--run-id", help="Run ID for monitor action")
    
    args = parser.parse_args()
    
    setup = DatabricksSetup(args.workspace_url, args.access_token, args.config_path)
    
    if args.action == "setup":
        result = setup.setup_complete_environment(
            args.cluster_name,
            args.job_name,
            args.local_files_path
        )
        print(f"Setup completed: {json.dumps(result, indent=2)}")
        
    elif args.action == "start":
        if not args.job_id:
            print("Job ID required for start action")
            return
        run_id = setup.start_job(args.job_id)
        print(f"Job started with run ID: {run_id}")
        
    elif args.action == "monitor":
        if not args.run_id:
            print("Run ID required for monitor action")
            return
        success = setup.monitor_job(args.run_id)
        print(f"Job monitoring completed. Success: {success}")

if __name__ == "__main__":
    main()
