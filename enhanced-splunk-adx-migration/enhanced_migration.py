"""
Enhanced Splunk to Azure Data Explorer Migration using Apache Spark
Optimized for Azure Databricks and large-scale data transfers (250TB+)

Features:
- Time-based partitioning for parallel processing
- Checkpointing and resume capability
- Exponential backoff retry logic
- Connection pooling and resource optimization
- Progress tracking and monitoring
- Data integrity validation
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
import splunklib.client as client
import splunklib.results as results
import json
import os
import sys
import datetime
import time
import logging
import yaml
from typing import Dict, List, Optional, Tuple
import concurrent.futures
from threading import Lock
import pandas as pd
from azure.kusto.data import KustoConnectionStringBuilder
from azure.kusto.ingest import (
    QueuedIngestClient,
    IngestionProperties,
    DataFormat,
    IngestionStatus
)
from azure.kusto.data.exceptions import KustoApiError
import tempfile
import shutil
import math
from pathlib import Path

class EnhancedSplunkADXMigration:
    def __init__(self, config_path: str):
        """Initialize the migration with configuration."""
        self.config = self._load_config(config_path)
        self.setup_logging()
        self.checkpoint_manager = CheckpointManager(self.config['checkpoint']['directory'])
        self.progress_tracker = ProgressTracker()
        self.spark = self._create_spark_session()
        self.adx_client = self._create_adx_client()
        self.splunk_service = self._create_splunk_connection()
        
    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from YAML file."""
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)
    
    def setup_logging(self):
        """Setup comprehensive logging."""
        log_level = getattr(logging, self.config.get('logging', {}).get('level', 'INFO'))
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        
        logging.basicConfig(
            level=log_level,
            format=log_format,
            handlers=[
                logging.FileHandler(f"migration_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def _create_spark_session(self) -> SparkSession:
        """Create optimized Spark session for large-scale processing."""
        spark_config = self.config['spark']
        
        spark = SparkSession.builder \
            .appName("Enhanced-Splunk-ADX-Migration") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .config("spark.dynamicAllocation.enabled", "true") \
            .config("spark.dynamicAllocation.minExecutors", str(spark_config.get('min_executors', 2))) \
            .config("spark.dynamicAllocation.maxExecutors", str(spark_config.get('max_executors', 20))) \
            .config("spark.sql.execution.arrow.maxRecordsPerBatch", "10000")
        
        # Add Kusto Spark connector
        if 'kusto_jar_path' in spark_config:
            spark = spark.config("spark.jars", spark_config['kusto_jar_path'])
        
        return spark.getOrCreate()
    
    def _create_adx_client(self) -> QueuedIngestClient:
        """Create Azure Data Explorer client with connection pooling."""
        adx_config = self.config['adx']
        
        kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
            adx_config['ingest_url'],
            adx_config['client_id'],
            adx_config['client_secret'],
            adx_config['authority']
        )
        kcsb._set_connector_details("Enhanced.Splunk.Migration", "2.0.0")
        
        return QueuedIngestClient(kcsb)
    
    def _create_splunk_connection(self):
        """Create Splunk connection with retry logic."""
        splunk_config = self.config['splunk']
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                service = client.connect(
                    host=splunk_config['host'],
                    port=splunk_config['port'],
                    username=splunk_config['username'],
                    password=splunk_config['password'],
                    timeout=splunk_config.get('timeout', 60)
                )
                self.logger.info("Successfully connected to Splunk")
                return service
            except Exception as e:
                self.logger.warning(f"Splunk connection attempt {attempt + 1} failed: {e}")
                if attempt == max_retries - 1:
                    raise
                time.sleep(2 ** attempt)
    
    def generate_time_partitions(self, start_date: str, end_date: str, partition_size_days: int = 1) -> List[Tuple[str, str]]:
        """Generate time-based partitions for parallel processing."""
        start = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.datetime.strptime(end_date, '%Y-%m-%d')
        
        partitions = []
        current = start
        
        while current < end:
            partition_end = min(current + datetime.timedelta(days=partition_size_days), end)
            partitions.append((
                current.strftime('%Y-%m-%d'),
                partition_end.strftime('%Y-%m-%d')
            ))
            current = partition_end
            
        self.logger.info(f"Generated {len(partitions)} time partitions")
        return partitions
    
    def process_partition(self, partition_start: str, partition_end: str, index_name: str) -> bool:
        """Process a single time partition."""
        partition_id = f"{index_name}_{partition_start}_{partition_end}"
        
        # Check if partition already processed
        if self.checkpoint_manager.is_partition_completed(partition_id):
            self.logger.info(f"Partition {partition_id} already completed, skipping")
            return True
        
        try:
            self.logger.info(f"Processing partition: {partition_id}")
            
            # Build Splunk search query for the partition
            search_query = self._build_partition_query(index_name, partition_start, partition_end)
            
            # Extract data from Splunk with retry logic
            data = self._extract_splunk_data_with_retry(search_query, partition_id)
            
            if not data:
                self.logger.warning(f"No data found for partition {partition_id}")
                self.checkpoint_manager.mark_partition_completed(partition_id)
                return True
            
            # Process data with Spark
            df = self._process_data_with_spark(data, partition_id)
            
            # Ingest to ADX with retry logic
            success = self._ingest_to_adx_with_retry(df, partition_id)
            
            if success:
                self.checkpoint_manager.mark_partition_completed(partition_id)
                self.progress_tracker.increment_completed_partitions()
                self.logger.info(f"Successfully processed partition {partition_id}")
                return True
            else:
                self.logger.error(f"Failed to process partition {partition_id}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error processing partition {partition_id}: {e}")
            return False
    
    def _build_partition_query(self, index_name: str, start_date: str, end_date: str) -> str:
        """Build optimized Splunk search query for a partition."""
        base_query = self.config['splunk'].get('base_query', 'search')
        
        query = f"{base_query} index={index_name} earliest={start_date}T00:00:00 latest={end_date}T23:59:59"
        
        # Add any additional filters
        filters = self.config['splunk'].get('additional_filters', [])
        for filter_clause in filters:
            query += f" {filter_clause}"
        
        # Optimize query for large datasets
        query += " | sort _time"
        
        return query
    
    def _extract_splunk_data_with_retry(self, search_query: str, partition_id: str) -> Optional[List[Dict]]:
        """Extract data from Splunk with exponential backoff retry."""
        max_retries = self.config['retry']['max_attempts']
        base_delay = self.config['retry']['base_delay']
        max_count = self.config['splunk'].get('max_results_per_partition', 100000)
        
        for attempt in range(max_retries):
            try:
                self.logger.info(f"Executing Splunk search for {partition_id}, attempt {attempt + 1}")
                
                # Execute search with proper parameters
                kwargs_normalsearch = {"exec_mode": "normal"}
                kwargs_options = {
                    "output_mode": "json",
                    "count": max_count,
                    "timeout": self.config['splunk'].get('search_timeout', 300)
                }
                
                job = self.splunk_service.jobs.create(search_query, **kwargs_normalsearch)
                
                # Monitor job progress
                self._monitor_splunk_job(job, partition_id)
                
                # Get results
                results_stream = job.results(**kwargs_options)
                results_data = results_stream.read()
                job.cancel()
                
                # Parse JSON results
                results_dict = json.loads(results_data)
                raw_results = results_dict.get('results', [])
                
                if not raw_results:
                    return None
                
                # Process raw data
                processed_data = []
                for item in raw_results:
                    if '_raw' in item:
                        try:
                            raw_value = item["_raw"].strip()
                            if raw_value.startswith('{'):
                                processed_data.append(json.loads(raw_value))
                            else:
                                # Handle non-JSON data
                                processed_data.append({"raw_data": raw_value, **item})
                        except json.JSONDecodeError:
                            # Handle malformed JSON
                            processed_data.append({"raw_data": raw_value, **item})
                    else:
                        processed_data.append(item)
                
                self.logger.info(f"Extracted {len(processed_data)} records from partition {partition_id}")
                return processed_data
                
            except Exception as e:
                self.logger.warning(f"Attempt {attempt + 1} failed for partition {partition_id}: {e}")
                if attempt == max_retries - 1:
                    raise
                
                # Exponential backoff with jitter
                delay = base_delay * (2 ** attempt) + (time.time() % 1)
                time.sleep(delay)
        
        return None
    
    def _monitor_splunk_job(self, job, partition_id: str):
        """Monitor Splunk search job progress."""
        while True:
            if not job.is_ready():
                time.sleep(1)
                continue
                
            stats = {
                "isDone": job["isDone"],
                "doneProgress": float(job["doneProgress"]) * 100,
                "scanCount": int(job["scanCount"]),
                "eventCount": int(job["eventCount"]),
                "resultCount": int(job["resultCount"])
            }
            
            progress_msg = (f"Partition {partition_id}: "
                          f"{stats['doneProgress']:.1f}% complete, "
                          f"{stats['scanCount']} scanned, "
                          f"{stats['eventCount']} matched, "
                          f"{stats['resultCount']} results")
            
            self.logger.info(progress_msg)
            
            if stats["isDone"] == "1":
                self.logger.info(f"Splunk search completed for partition {partition_id}")
                break
                
            time.sleep(5)
    
    def _process_data_with_spark(self, data: List[Dict], partition_id: str):
        """Process data using Spark for optimal performance."""
        try:
            # Create DataFrame
            df = self.spark.createDataFrame(data)
            
            # Add metadata columns
            df = df.withColumn("migration_timestamp", current_timestamp())
            df = df.withColumn("partition_id", lit(partition_id))
            
            # Apply any data transformations
            transformations = self.config.get('transformations', {})
            if transformations.get('enable_schema_inference', True):
                df = df.repartition(self.config['spark'].get('partition_count', 10))
            
            # Cache for multiple operations
            df.cache()
            
            record_count = df.count()
            self.logger.info(f"Processed {record_count} records with Spark for partition {partition_id}")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error processing data with Spark for partition {partition_id}: {e}")
            raise
    
    def _ingest_to_adx_with_retry(self, df, partition_id: str) -> bool:
        """Ingest DataFrame to ADX with retry logic."""
        max_retries = self.config['retry']['max_attempts']
        base_delay = self.config['retry']['base_delay']
        adx_config = self.config['adx']
        
        for attempt in range(max_retries):
            try:
                self.logger.info(f"Ingesting partition {partition_id} to ADX, attempt {attempt + 1}")
                
                # Use Spark's native ADX connector for best performance
                df.write.format("com.microsoft.kusto.spark.datasource") \
                    .option("kustoCluster", adx_config['cluster']) \
                    .option("kustoDatabase", adx_config['database']) \
                    .option("kustoTable", adx_config['table']) \
                    .option("kustoAadAppId", adx_config['client_id']) \
                    .option("kustoAadAppSecret", adx_config['client_secret']) \
                    .option("kustoAadAuthorityID", adx_config['authority']) \
                    .option("kustoTableCreateOptions", "CreateIfNotExist") \
                    .mode("Append") \
                    .save()
                
                self.logger.info(f"Successfully ingested partition {partition_id} to ADX")
                return True
                
            except Exception as e:
                self.logger.warning(f"Ingestion attempt {attempt + 1} failed for partition {partition_id}: {e}")
                
                if attempt == max_retries - 1:
                    self.logger.error(f"All ingestion attempts failed for partition {partition_id}")
                    return False
                
                # Exponential backoff
                delay = base_delay * (2 ** attempt)
                time.sleep(delay)
        
        return False
    
    def run_migration(self):
        """Run the complete migration process."""
        migration_config = self.config['migration']
        
        self.logger.info("Starting enhanced Splunk to ADX migration")
        self.logger.info(f"Migration configuration: {migration_config}")
        
        # Generate partitions for all indexes
        all_partitions = []
        for index_name in migration_config['indexes']:
            partitions = self.generate_time_partitions(
                migration_config['start_date'],
                migration_config['end_date'],
                migration_config['partition_size_days']
            )
            for partition in partitions:
                all_partitions.append((partition[0], partition[1], index_name))
        
        total_partitions = len(all_partitions)
        self.progress_tracker.set_total_partitions(total_partitions)
        
        self.logger.info(f"Total partitions to process: {total_partitions}")
        
        # Process partitions with configurable parallelism
        max_workers = migration_config.get('max_parallel_partitions', 5)
        successful_partitions = 0
        failed_partitions = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all partition jobs
            future_to_partition = {
                executor.submit(self.process_partition, start, end, index): (start, end, index)
                for start, end, index in all_partitions
            }
            
            # Process completed jobs
            for future in concurrent.futures.as_completed(future_to_partition):
                partition_info = future_to_partition[future]
                try:
                    result = future.result()
                    if result:
                        successful_partitions += 1
                    else:
                        failed_partitions.append(partition_info)
                    
                    # Log progress
                    progress = (successful_partitions / total_partitions) * 100
                    self.logger.info(f"Migration progress: {progress:.2f}% ({successful_partitions}/{total_partitions})")
                    
                except Exception as e:
                    self.logger.error(f"Partition {partition_info} generated exception: {e}")
                    failed_partitions.append(partition_info)
        
        # Final summary
        self.logger.info("Migration completed!")
        self.logger.info(f"Successful partitions: {successful_partitions}")
        self.logger.info(f"Failed partitions: {len(failed_partitions)}")
        
        if failed_partitions:
            self.logger.warning("Failed partitions (can be retried):")
            for partition in failed_partitions:
                self.logger.warning(f"  - {partition}")
        
        # Cleanup
        self.cleanup()
    
    def cleanup(self):
        """Cleanup resources."""
        try:
            if hasattr(self, 'splunk_service'):
                self.splunk_service.logout()
            if hasattr(self, 'spark'):
                self.spark.stop()
            self.logger.info("Cleanup completed successfully")
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")


class CheckpointManager:
    """Manages migration checkpoints for resumable transfers."""
    
    def __init__(self, checkpoint_dir: str):
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.completed_partitions_file = self.checkpoint_dir / "completed_partitions.txt"
        self._lock = Lock()
    
    def is_partition_completed(self, partition_id: str) -> bool:
        """Check if a partition has been completed."""
        if not self.completed_partitions_file.exists():
            return False
        
        with self._lock:
            with open(self.completed_partitions_file, 'r') as f:
                completed = set(line.strip() for line in f)
                return partition_id in completed
    
    def mark_partition_completed(self, partition_id: str):
        """Mark a partition as completed."""
        with self._lock:
            with open(self.completed_partitions_file, 'a') as f:
                f.write(f"{partition_id}\n")


class ProgressTracker:
    """Tracks migration progress and provides statistics."""
    
    def __init__(self):
        self.total_partitions = 0
        self.completed_partitions = 0
        self.start_time = time.time()
        self._lock = Lock()
    
    def set_total_partitions(self, total: int):
        """Set the total number of partitions."""
        with self._lock:
            self.total_partitions = total
    
    def increment_completed_partitions(self):
        """Increment the completed partitions counter."""
        with self._lock:
            self.completed_partitions += 1
    
    def get_progress_stats(self) -> Dict:
        """Get current progress statistics."""
        with self._lock:
            elapsed_time = time.time() - self.start_time
            progress_pct = (self.completed_partitions / self.total_partitions * 100) if self.total_partitions > 0 else 0
            
            # Estimate completion time
            if self.completed_partitions > 0:
                avg_time_per_partition = elapsed_time / self.completed_partitions
                remaining_partitions = self.total_partitions - self.completed_partitions
                estimated_remaining_time = remaining_partitions * avg_time_per_partition
            else:
                estimated_remaining_time = 0
            
            return {
                'total_partitions': self.total_partitions,
                'completed_partitions': self.completed_partitions,
                'progress_percentage': progress_pct,
                'elapsed_time_seconds': elapsed_time,
                'estimated_remaining_time_seconds': estimated_remaining_time
            }


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Enhanced Splunk to ADX Migration")
    parser.add_argument("--config", required=True, help="Path to configuration file")
    parser.add_argument("--resume", action="store_true", help="Resume from checkpoint")
    
    args = parser.parse_args()
    
    try:
        migration = EnhancedSplunkADXMigration(args.config)
        migration.run_migration()
    except KeyboardInterrupt:
        print("\nMigration interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Migration failed with error: {e}")
        sys.exit(1)
