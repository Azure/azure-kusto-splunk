import json
import os
import time
import tempfile
import shutil
import random
import io
import gzip

from azure.kusto.data import DataFormat, KustoConnectionStringBuilder
from azure.kusto.data.data_format import IngestionMappingKind
from azure.kusto.ingest import (FileDescriptor, IngestionProperties,
                                IngestionStatus, QueuedIngestClient,
                                ReportLevel, StreamDescriptor)
from azure.kusto.data.exceptions import KustoApiError
from azure.kusto.data.exceptions import KustoMultiApiError

max_size = 5 * 1024 * 1024 #5MB
retry_times = 5
retry_interval = 5

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
        self.exponential_backoff=True



    def send(self, helper, events):
        timestr = time.strftime("%Y%m%d-%H%M%S")
        buffer_file_name = 'ADXIngest-' + timestr + '.json.gz'
        for event in events:
            if helper.get_param("durable_mode") == "1" :
                #durable mode ingestion : first we write to buffer and later we ingest them to ADX
                try:
                    parentdir = os.getcwd()
                    buffer_folder_name = os.path.join(parentdir,"ADX_Ingest")
                    #create buffer folder if not exists
                    if not os.path.isdir(buffer_folder_name):
                        os.makedirs(buffer_folder_name)
                    #create buffer file if not exists    
                    if not os.path.isfile(os.path.join(buffer_folder_name,buffer_file_name)):
                        open(os.path.join(buffer_folder_name,buffer_file_name), 'a').close()
                    #check if buffer file size is greater than 5MB, if yes then create new file    
                    if os.path.getsize(os.path.join(buffer_folder_name,buffer_file_name)) > max_size:
                        helper.log_info("max size exceeded = {}")
                        timestr = time.strftime("%Y%m%d-%H%M%S")
                        buffer_file_name = 'ADXIngest-' + timestr + '.json.gz'
                        with gzip.open(os.path.join(buffer_folder_name,buffer_file_name), 'wb') as outfile:
                            #json.dump(event,outfile)
                            outfile.write(json.dumps(event).encode('utf-8'))
                            self.successfully_sent_events_number += 1
                        self.ingest_to_adx(helper)       
                    else:
                        #write to buffer file
                        with gzip.open(os.path.join(buffer_folder_name,buffer_file_name),'ab') as outfile:
                            outfile.write('\n'.encode('utf-8'))
                            outfile.write(json.dumps(event).encode('utf-8'))
                            #json.dump(event,outfile)
                            self.successfully_sent_events_number += 1
                except Exception as err:
                    helper.log_error('Error while writing data to buffer in bin. {}'.format(err))  
            else :
                #non-durable ingestion mode where the incoming events are directly ingested to ADX
                retry_count = 0
                while retry_count < retry_times:
                    try:
                        event_str = json.dumps(event)
                        json_bytes = event_str.encode('utf-8')
                        stream = io.BytesIO(json_bytes)
                        stream_descriptor = StreamDescriptor(stream)
                        self.client.ingest_from_stream(stream_descriptor, ingestion_properties=self.ingestion_props)
                        self.successfully_sent_events_number += 1
                        break # Event posted to ADX, break out of the retry loop
                    except Exception as ex:
                        helper.log_error('Session= {} Error while sending data to ADX. {}'.format(self.session_identifier,ex))
                        if isinstance(ex, KustoApiError) and ex.get_api_error().permanent:
                            helper.log_error('Session= {} Error while sending data to ADX due to KustoApiError - Permanent Error. {}'.format(self.session_identifier,ex))
                            break; # Permanent error, break out of the retry loop
                        if isinstance(ex, KustoMultiApiError) and ex.get_api_errors()[0].permanent:
                            helper.log_error('Session= {} Error while sending data to ADX due to KustoMultiApiError - Permanent Error {}'.format(self.session_identifier,ex))
                            break; # Permanent error, break out of the retry loop
                        retry_count += 1
                        helper.log_error('Session= {} Error while sending data to ADX. Retrying... {}'.format(self.session_identifier,ex))
                        time.sleep(retry_interval * (2 ** retry_count) + random.randint(1,10)) # Exponential backoff with jitter
                else:
                    helper.log_error('Session= {} . Failed to ingest event even after {} retries'.format(self.session_identifier,self.retry_times))   
        self.ingest_to_adx(helper)

    def ingest_to_adx(self, helper):
        #if durable mode is disabled, then return
        if(helper.get_param('durable_mode') == "0"):
            return 0
        time.sleep(2) #wait for 2 sec before ingesting to ADX
        # Set the folder path to read the JSON files 
        folder_path = os.getcwd()
        buffer_folder_name = os.path.join(folder_path, "ADX_Ingest")

        # Iterate through the JSON files in the directory
        for file in self._get_files_not_modified(buffer_folder_name):
            file_path = os.path.join(buffer_folder_name, file)
            helper.log_info("Processing file = {}".format(file_path))
            if file.endswith('.json.gz') and self._is_file_not_being_modified(file_path) :
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
                     helper.log_error("File Ingestion failed, will retry ingestion in the next event invocation = {}".format(file_path))
                     continue
    

    def _get_files_not_modified(self,directory):
        current_time = time.time()
        files = []
        for file_name in os.listdir(directory):
            file_path = os.path.join(directory, file_name)
            # Check if it is a regular file
            if os.path.isfile(file_path):
                # Get the file's modification time
                modification_time = os.path.getmtime(file_path)
                # Compare the modification time with the current time
                if modification_time < current_time:
                    files.append(file_path)
        return files
    

    def _move_files_to_temp_dir(self,helper,source_file_path):
        #create temp dir if not exists
        if not os.path.isdir(os.path.join(tempfile.gettempdir(), "ADX_Ingest")):
            temp_dir_created = tempfile.mkdtemp()
            temp_dir = os.path.join(os.path.dirname(temp_dir_created), "ADX_Ingest")
            os.rename(temp_dir_created, temp_dir)
        else:
            temp_dir = os.path.join(tempfile.gettempdir(), "ADX_Ingest")
        if os.path.isfile(os.path.join(temp_dir, os.path.basename(source_file_path))):
            new_file_name = self._get_new_file_name(os.path.basename(source_file_path))
            shutil.move(source_file_path, os.path.join(temp_dir, new_file_name))
        else:
            shutil.move(source_file_path, temp_dir)

    def _get_new_file_name(self,file_name):
        base_name, ext = os.path.splitext(file_name)
        counter = 1
        new_file_name = f"{base_name}_{counter}{ext}"
        while os.path.exists(os.path.join(new_file_name)):
            counter += 1
            new_file_name = f"{base_name}_{counter}{ext}"
        return new_file_name    

    def _is_file_not_being_modified(self,file_path):
        """
        Returns True if the file is currently being not modified, False otherwise.
        """
        if os.path.getsize(file_path) > max_size:
            return True
        else : 
            file_stat = os.stat(file_path)
            current_size = file_stat.st_size
            time.sleep(10)  # Wait for 5 second
            new_size = os.stat(file_path).st_size
            # Get the file's modification time
            modification_time = os.path.getmtime(file_path)
            # Compare the modification time with the current time
            current_time = time.time()
            if modification_time < current_time and current_size == new_size:
                return True
            else:
                return False



