from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from azure.kusto.data.helpers import dataframe_from_result_table
from azure.kusto.data import KustoConnectionStringBuilder
import splunklib.client as client
import splunklib.results as results
import io, os, sys, types, datetime, math, time, glob, json
from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder


# Connection parameters
host = "localhost"
port = 8089
username = "admin"
password = "admin123"

cluster = "adx-cluster"
database = "database-name"
client_id = "client-id"
client_secret = "client-secret"
authority = "authority"
table_name = "table-name"
table_mapping_name = "table-name-mapping"


# Connect to the Splunk instance
service = client.connect(
    host=host,
    port=port,
    username=username,
    password=password
)

spark = SparkSession.builder.appName("SplunkToADX").config("spark.jars", "/path/to/kusto-spark_3.0_2.12-3.1.15-jar-with-dependencies.jar").getOrCreate()

# Define the search query
splunk_query = 'search index=devtutorial | head 50'

kwargs_normalsearch={"exec_mode": "normal"}
kwargs_options={"output_mode": "json", "count": 1000000}


# Execute Search
job = service.jobs.create(splunk_query, **kwargs_normalsearch)

# A normal search returns the job's SID right away, so we need to poll for completion
while True:
    while not job.is_ready():
        pass
    stats = {"isDone": job["isDone"], "doneProgress": float(job["doneProgress"])*100,
             "scanCount": int(job["scanCount"]), "eventCount": int(job["eventCount"]),
             "resultCount": int(job["resultCount"])}
    status = ("\r%(doneProgress)03.1f%%   %(scanCount)d scanned   "
              "%(eventCount)d matched   %(resultCount)d results") % stats

    sys.stdout.write(status + '\n')
    sys.stdout.flush()
    if stats["isDone"] == "1":
        sys.stdout.write("\nDone!")
        break
    time.sleep(0.5)

# Get the results and display them
results = job.results(**kwargs_options).read()
job.cancel()

results_dict = json.loads(results)

#print(results_dict)

results_data = results_dict['results']
#print(results_data)

new_json = []

for item in results_data:
    raw_value = item["_raw"].strip()
    new_json.append(json.loads(raw_value))

new_json_string = json.dumps(new_json, indent=4)

data_list = json.loads(new_json_string)

data_frame = spark.createDataFrame(data_list)

print(data_frame)

data_frame.write.format("com.microsoft.kusto.spark.datasource"). \
    option("kustoCluster", cluster). \
    option("kustoDatabase", database). \
    option("kustoTable", table_name). \
    option("kustoAadAppId",client_id). \
    option("kustoAadAppSecret",client_secret). \
    option("kustoAadAuthorityID",authority). \
    mode("Append").save()

service.logout()

spark.stop()