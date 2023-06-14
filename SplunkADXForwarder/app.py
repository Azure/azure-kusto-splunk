import socket
import logging
import time
import os
import yaml
import pandas as pd
import csv
import threading
import tempfile
import shutil
from azure.kusto.data import KustoConnectionStringBuilder
from azure.kusto.data.data_format import DataFormat
from azure.kusto.ingest import (
    IngestionProperties,
    IngestionStatus,
    QueuedIngestClient
)

# Constants
MAX_BATCH_SIZE = int(os.environ.get('MAX_BATCH_SIZE', '1000'))
MAX_WAIT_TIME_SECONDS = int(os.environ.get('MAX_WAIT_TIME_SECONDS', '5'))
MAX_RETRIES = int(os.environ.get('MAX_RETRIES', '3'))
MAX_SIZE = 5 * 1024 * 1024 #5MB
DELETE_TEMP_FILE_DAYS_THRESHOLD = 1

def is_file_not_being_modified(file_path):
    """
    Returns True if the file is currently being not modified, False otherwise.
    """
    file_stat = os.stat(file_path)
    current_size = file_stat.st_size
    time.sleep(1)  # Wait for 1 second
    new_size = os.stat(file_path).st_size
    if current_size != new_size:
        logging.info("the file is currently being modified")
    return current_size == new_size
    

def ingest_data(conn_string_builder: KustoConnectionStringBuilder, database: str, table: str, table_mapping: str) -> bool:
    """Ingest the data into Azure Data Explorer."""
    while True:
         # Set the folder path to read the JSON files
        time.sleep(60)
        folder_path = os.getcwd()
        buffer_folder_name = os.path.join(folder_path, "ADX_Ingest")
        logging.info(f'ingesting message from {buffer_folder_name}')

        #files = os.listdir(buffer_folder_name)

        ingestion_client = QueuedIngestClient(conn_string_builder)
        ingest_props = IngestionProperties(database=database, table=table, flush_immediately=False, data_format=DataFormat.CSV, ingestion_mapping_reference=table_mapping)

        if os.path.isdir(buffer_folder_name):
            for file in get_files_not_modified(buffer_folder_name):
                file_path = os.path.join(buffer_folder_name, file)
                logging.info(f"Processing file :: {file_path}")
                if file.endswith('.csv') and is_file_not_being_modified(file_path):
                    # ingest from file
                    df = pd.read_csv(file_path)
                    result = ingestion_client.ingest_from_dataframe(df, ingestion_properties=ingest_props)
                    #if ingestion result is success or queued, then move the file to temp folder
                    if result.status == IngestionStatus.QUEUED or result.status == IngestionStatus.SUCCESS:
                        try:
                            move_files_to_temp_dir(file_path)
                            logging.info("File posted to ADX and Moved file to temp folder {}".format(file_path))
                        except Exception as err:
                            #if any error, continue, the file will be picked up in the next event invocation
                            logging.error("Error while moving file to temp folder {}".format(err))
                            continue
                    else:
                        logging.error("File ingestion failed, will retry ingestion in the next attempt {}".format(file_path))
                        continue         
        else:
            logging.error(f"The folder {buffer_folder_name} doesnt exist")

def move_files_to_temp_dir(source_file_path):
        if not os.path.isdir(os.path.join(tempfile.gettempdir(), "ADX_Ingest")):
            temp_dir_created = tempfile.mkdtemp()
            temp_dir = os.path.join(os.path.dirname(temp_dir_created), "ADX_Ingest")
            os.rename(temp_dir_created, temp_dir)
        else:
            temp_dir = os.path.join(tempfile.gettempdir(), "ADX_Ingest")
        if os.path.isfile(os.path.join(temp_dir, os.path.basename(source_file_path))):
            new_file_name = get_new_file_name(os.path.basename(source_file_path))
            shutil.move(source_file_path, os.path.join(temp_dir, new_file_name))
        else:
            shutil.move(source_file_path, temp_dir)


def get_new_file_name(file_name):
        base_name, ext = os.path.splitext(file_name)
        counter = 1
        new_file_name = f"{base_name}_{counter}{ext}"
        while os.path.exists(os.path.join(new_file_name)):
            counter += 1
            new_file_name = f"{base_name}_{counter}{ext}"
        return new_file_name            

def get_files_not_modified(directory):
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

def is_file_not_being_modified(file_path):
        """
        Returns True if the file is currently being not modified, False otherwise.
        """
        if os.path.getsize(file_path) > MAX_SIZE:
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

def listen_for_messages(host: str, port: int):
    """Listen for incoming messages on the specified host and port."""
    logging.info(f'Listening for incoming messages on {host}:{port}...')
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((host, port))
        s.listen()
        while True:
            conn, addr = s.accept()
            with conn:
                logging.info(f'Connected by {addr}')
                data = b''
                start_time = time.monotonic()
                while True:
                    chunk = conn.recv(1024)
                    if not chunk:
                        break
                    data += chunk
                    if len(data) >= MAX_BATCH_SIZE:
                        data = b''
                        start_time = time.monotonic()
                    if time.monotonic() - start_time > MAX_WAIT_TIME_SECONDS:
                        break
                #logging.info(f'Chunk log after parse {len(data.decode().splitlines())}')
                if len(data.decode().splitlines()) > 0 :
                    write_to_file(data=data.decode())

def create_file(file_path):
    try:
        file = open(file_path, "x")
        file.close()
        logging.info(f"File '{file_path}' created successfully.")
    except FileExistsError:
        logging.error(f"File '{file_path}' already exists.")


def remove_empty_lines(text):
    # Split the text into lines
    lines = text.split(os.linesep)
    # Filter out empty lines
    non_empty_lines = [line for line in lines if line.strip()]
    # Join the non-empty lines back into a string
    result = os.linesep.join(non_empty_lines)
    return result

def write_to_file(data: str):
        data = remove_empty_lines(data)
        try:
            timestr = time.strftime("%Y%m%d-%H%M%S")
            buffer_file_name = 'ADXIngest-' + timestr + '.csv'
            parentdir = os.getcwd()
            buffer_folder_name = os.path.join(parentdir, "ADX_Ingest")
            logging.info(f'Folder Path for writing Buffer Data is. {buffer_folder_name} and {buffer_file_name}')

            if not os.path.isdir(buffer_folder_name):
                logging.info(f'Creating folder :: {buffer_folder_name} ')
                os.makedirs(buffer_folder_name)

            create_file(os.path.join(buffer_folder_name,buffer_file_name))
            with open(os.path.join(buffer_folder_name, buffer_file_name), 'w+') as file:
                writer = csv.writer(file)
                for line in data.split(os.linesep):
                    writer.writerow([line])

        except Exception as err:
            logging.error('Error while writing data to buffer in bin. {}'.format(err))

def delete_files_older_than(directory, days):
    current_time = time.time()
    seconds_in_day = 24 * 60 * 60
    threshold = current_time - (days * seconds_in_day)

    #for first run, the directory may not exist, hence skipping execution
    if not os.path.isdir(directory):
        return 0

    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        if os.path.isfile(file_path):
            modification_time = os.path.getmtime(file_path)
            if modification_time < threshold:
                os.remove(file_path)
                logging.info(f"Deleted file: {file_path}")

def delete_older_temp_files():
    while True:
          delete_files_older_than(os.path.join(tempfile.gettempdir(), "ADX_Ingest"), DELETE_TEMP_FILE_DAYS_THRESHOLD)
          time.sleep(60*60) # Sleep for 1 hour before checking again                   


if __name__ == '__main__':
    with open("config.yml", "r") as config_file:
        config = yaml.safe_load(config_file)

    # Initialize Azure Data Explorer client
    cluster = config['ingest_url']
    database = config['database_name']
    client_id = config['client_id']
    client_secret = config['client_secret']
    authority = config['authority']
    table_name = config['table_name']
    table_mapping_name = config['table_mapping_name']

    # Configure logging
    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=logging.INFO)
    kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(cluster, client_id, client_secret,
                                                                                authority)
    
    timer_thread = threading.Thread(target=ingest_data, args=(kcsb,database, table_name, table_mapping_name))
    timer_thread.start()

    temp_file_delete_thread = threading.Thread(target=delete_older_temp_files)
    temp_file_delete_thread.start()

    # Listen for incoming messages
    listen_for_messages('0.0.0.0', 9997)
