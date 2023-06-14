import io
import json
import os
import random
import time
import tempfile
import shutil

from azure.kusto.data import DataFormat, KustoConnectionStringBuilder
from azure.kusto.data.data_format import IngestionMappingKind
from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.ingest import (FileDescriptor, IngestionProperties,
                                IngestionStatus, QueuedIngestClient,
                                ReportLevel, StreamDescriptor)
from azure.kusto.ingest.descriptors import StreamDescriptor


class ADXSplunkConnector:
    def __init__(self, ingest_URL, app_id, app_secret, tenant_id, database_name, table_name, table_mapping_name, durable_mode, session_identifier):
        self.ingest_URL = ingest_URL
        self.app_id = app_id
        self.app_secret = app_secret
        self.tenant_id = tenant_id
        self.database_name = database_name
        self.table_name = table_name
        self.table_mapping_name = table_mapping_name
        self.successfully_sent_events_number = 0
        self.send_failure_events_number = 0
        self.session_identifier = session_identifier
        self.durable_mode = durable_mode
        kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(self.ingest_URL, self.app_id,
                                                                                self.app_secret, self.tenant_id)

        client = QueuedIngestClient(kcsb)
        ingestion_props = IngestionProperties(
            database=self.database_name,
            table=self.table_name,
            data_format=DataFormat.JSON,
            report_level=ReportLevel.FailuresAndSuccesses,
            ingestion_mapping_reference=self.table_mapping_name,
            flush_immediately=False,
            ingestion_mapping_kind=IngestionMappingKind.JSON
        )
        self.client = client
        self.ingestion_props = ingestion_props
        self.retry_times=5, 
        self.retry_interval=5
        self.exponential_backoff=True



    def send(self, helper, events):
        for event in events:
            if helper.get_param("durable_mode") == "1" :
                #durable mode ingestion : first we write to buffer and later we ingest them to ADX
                try:
                    timestr = time.strftime("%Y%m%d-%H%M")
                    buffer_file_name = 'ADXIngest-' + timestr + '.json'
                    parentdir = os.getcwd()
                    buffer_folder_name = os.path.join(parentdir,"ADX_Ingest")
                    if not os.path.isdir(buffer_folder_name):
                        os.makedirs(buffer_folder_name)
                    if not os.path.isfile(os.path.join(buffer_folder_name,buffer_file_name)):
                        with open(os.path.join(buffer_folder_name,buffer_file_name), 'w+') as outfile:
                            json.dump(event,outfile)
                            self.successfully_sent_events_number += 1
                    else:
                        with open(os.path.join(buffer_folder_name,buffer_file_name), 'a+') as outfile:
                            outfile.write('\n')
                            json.dump(event,outfile)
                            self.successfully_sent_events_number += 1
                except Exception as err:
                    helper.log_error('Error while writing data to buffer in bin. {}'.format(err))
                
            else :
                #non-durable ingestion mode where the incoming events are directly ingested to ADX
                retry_count = 0
                while retry_count < self.retry_times:
                    try:
                        event_str = json.dumps(event)
                        json_bytes = event_str.encode('utf-8')
                        stream = io.BytesIO(json_bytes)
                        stream_descriptor = StreamDescriptor(stream)
                        self.client.ingest_from_stream(stream_descriptor, ingestion_properties=self.ingestion_props)
                        self.successfully_sent_events_number += 1
                        break # Event posted to ADX, break out of the retry loop
                    except KustoServiceError as ex:
                        helper.log_error('Session= {} Error while sending data to ADX. {}'.format(self.session_identifier,ex))
                        retry_count += 1
                        time.sleep(self.retry_interval * (2 ** retry_count) + random.randint(1,10)) # Exponential backoff with jitter
                else:
                    helper.log_error('Session= {} . Failed to ingest event even after {} retries'.format(self.session_identifier,self.retry_times))
        
        #ingest to adx
        self.ingest_to_adx(helper)    

    def ingest_to_adx(self, helper):
        if(helper.get_param('durable_mode') == '0'):
            return 0
    
        # Set the folder path to read the JSON files 
        folder_path = os.getcwd()
        buffer_folder_name = os.path.join(folder_path, "ADX_Ingest")

        files = os.listdir(buffer_folder_name)

        # Get the most recently modified file
        last_modified_file = max(files, key=lambda f: os.path.getmtime(os.path.join(buffer_folder_name, f)))

        # Select all files except the last modified one
        selected_files = [file for file in files if file != last_modified_file]

        # Iterate through the JSON files in the directory
        for file in selected_files:
            file_path = os.path.join(buffer_folder_name, file)
            helper.log_info("Processing file = {}".format(file_path))
            if file.endswith('.json'):
                # ingest from file
                file_descriptor = FileDescriptor(path=file_path)
                result = self.client.ingest_from_file(file_descriptor=file_descriptor, ingestion_properties=self.ingestion_props)
                #if ingestion result is success or queued, then move the file to temp folder
                if result.status == IngestionStatus.QUEUED or result.status == IngestionStatus.SUCCESS:
                    try:
                        self._move_files_to_temp_dir(helper, file_path)
                        helper.log_info("File posted to ADX and Moved file to temp folder {}".format(file_path))
                    except Exception as err:
                        #if any error, continue, the file will be picked up in the next event invocation
                        helper.log_error("Error while moving file to temp folder {}".format(err))
                        continue
                else:
                     helper.log.error("File Ingestion failed, will retry ingestion in the next event invocation = {}".format(file.name))
                     continue
    
    def _move_files_to_temp_dir(self,helper, file_path):
        if not os.path.isdir(os.path.join(tempfile.gettempdir(), "ADX_Ingest")):
            temp_dir_created = tempfile.mkdtemp()
            temp_dir = os.path.join(os.path.dirname(temp_dir_created), "ADX_Ingest")
            os.rename(temp_dir_created, temp_dir)
        else:
            temp_dir = os.path.join(tempfile.gettempdir(), "ADX_Ingest")
        helper.log_info("Temp Dir = {}".format(temp_dir))
        shutil.move(file_path, temp_dir)    

    def _is_file_not_being_modified(self,file_path):
        """
        Returns True if the file is currently being not modified, False otherwise.
        """
        file_stat = os.stat(file_path)
        current_size = file_stat.st_size
        time.sleep(60)  # Wait for 1 second
        new_size = os.stat(file_path).st_size
        return current_size == new_size



