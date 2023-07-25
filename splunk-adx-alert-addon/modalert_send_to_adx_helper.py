# encoding = utf-8

import base64
import datetime
import os
import json
from adx_connector import ADXSplunkConnector

OK = 0
ERROR_CODE_VALIDATION_FAILED = 1
ERROR_CODE_KUSTO_ENDPOINT_NOT_FOUND = 2
ERROR_CODE_AUTH_TOKEN_RETRIEVAL_FAILURE = 3
ERROR_CODE_INGESTION_FAILURE = 4


def remove_emptymvfields(value):
    if isinstance(value, list):
        return [remove_emptymvfields(x) for x in value if x != None and x != '']
    elif isinstance(value, dict):
        return {
            key: remove_emptymvfields(val)
            for key, val in value.items()
            if not (key.startswith("__mv_") and (val==None or val==''))
        }
    else:
        return value


def remove_mvfields(value):
    if isinstance(value, list):
        return [remove_mvfields(x) for x in value]
    elif isinstance(value, dict):
        return {
            key: remove_mvfields(val)
            for key, val in value.items()
            if not (key.startswith("__mv_"))
        }
    else:
        return value


def remove_emptyfields(value):
    if isinstance(value, list):
        return [remove_emptyfields(x) for x in value if x != None and x != '']
    elif isinstance(value, dict):
        return {
            key: remove_emptyfields(val)
            for key, val in value.items()
            if not (val==None or val=='')
        }
    else:
        return value


def remove_rawdatafields(value):
    if isinstance(value, list):
        return [remove_rawdatafields(x) for x in value]
    elif isinstance(value, dict):
        return {
            key: remove_rawdatafields(val)
            for key, val in value.items()
            if not (key == "_raw")
        }
    else:
        return value

def create_ADX_client(cluster_endpoint, app_id, app_secret, tenant_id, database, table, mapping_name, durable_mode, session_identifier):
    return ADXSplunkConnector(cluster_endpoint, app_id, app_secret, tenant_id, database, table, mapping_name, durable_mode, session_identifier)    

def process_event(helper, *args, **kwargs):
    """
    # IMPORTANT
    # Do not remove the anchor macro:start and macro:end lines.
    # These lines are used to generate sample code. If they are
    # removed, the sample code will not be updated when configurations
    # are updated.

    [sample_code_macro:start]

    # The following example gets the alert action parameters and prints them to the log
    cluster_url = helper.get_param("cluster_url")
    helper.log_info("cluster_url={}".format(cluster_url))

    app_id = helper.get_param("app_id")
    helper.log_info("app_id={}".format(app_id))

    app_secret = helper.get_param("app_secret")
    helper.log_info("app_secret={}".format(app_secret))

    tenant_id = helper.get_param("tenant_id")
    helper.log_info("tenant_id={}".format(tenant_id))

    database_name = helper.get_param("database_name")
    helper.log_info("database_name={}".format(database_name))

    table_name = helper.get_param("table_name")
    helper.log_info("table_name={}".format(table_name))

    table_mapping_name = helper.get_param("table_mapping_name")
    helper.log_info("table_mapping_name={}".format(table_mapping_name))


    # The following example adds two sample events ("hello", "world")
    # and writes them to Splunk
    # NOTE: Call helper.writeevents() only once after all events
    # have been added
    helper.addevent("hello", sourcetype="sample_sourcetype")
    helper.addevent("world", sourcetype="sample_sourcetype")
    helper.writeevents(index="summary", host="localhost", source="localhost")

    # The following example gets the events that trigger the alert
    events = helper.get_events()
    for event in events:
        helper.log_info("event={}".format(event))

    # helper.settings is a dict that includes environment configuration
    # Example usage: helper.settings["server_uri"]
    helper.log_info("server_uri={}".format(helper.settings["server_uri"]))
    [sample_code_macro:end]
    """

    helper.log_info("Alert action send_to_adx started.")

     # Azure AD tenant ID, client ID, and client secret
    tenant_id = helper.get_param('tenant_id')
    app_id = helper.get_param('app_id')
    app_secret = helper.get_param('app_secret')

    # Azure Data Explorer ingest URL
    cluster_endpoint = helper.get_param('cluster_url')

    #Azure Data Explorer database name
    database = helper.get_param('database_name')

    # Azure Data Explorer table name
    table = helper.get_param('table_name')

    # Azure Data Explorer table mapping name
    mapping_name = helper.get_param('table_mapping_name')

    # Azure Data Explorer ingestion durable mode
    durable_mode = helper.get_param('durable_mode')

    script_start_time_utc = datetime.datetime.utcnow()
    session_identifier = base64.b64encode(os.urandom(16))

    remove_extra_fields = helper.get_param("remove_extra_fields")

    events = helper.get_events()
    if (remove_extra_fields == "1"):
        helper.log_info("Session= {} Remove all __mv_ fields.".format(session_identifier))
        remove_mvfields_start_time_utc = datetime.datetime.utcnow()
        events = remove_mvfields(list(events))
        helper.log_info("Session= {} Time_taken_to_remove_mvfields={}".format(session_identifier,(datetime.datetime.utcnow() - remove_mvfields_start_time_utc).seconds))

    if (remove_extra_fields == "1"):
        helper.log_info("Session= {} Remove all __emptyfields_ fields.".format(session_identifier))
        remove_emptyfields_start_time_utc = datetime.datetime.utcnow()
        events = remove_emptyfields(list(events))
        helper.log_info("Session= {} Time_taken_to_remove_emptyfields={}".format(session_identifier,(datetime.datetime.utcnow() - remove_emptyfields_start_time_utc).seconds))


    if (remove_extra_fields == "1"):
        helper.log_info("Session= {} Remove all __emptymv_ fields.".format(session_identifier))
        remove_emptymvfields_start_time_utc = datetime.datetime.utcnow()
        events = remove_emptymvfields(list(events))
        helper.log_info("Session= {} Time_taken_to_remove_emptymvfields={}".format(session_identifier,(datetime.datetime.utcnow() - remove_emptymvfields_start_time_utc).seconds))


    if (remove_extra_fields == "1"):
        helper.log_info("Session= {} Remove all __rawdata_ fields.".format(session_identifier))
        remove_rawdatafields_start_time_utc = datetime.datetime.utcnow()
        events = remove_rawdatafields(list(events))
        helper.log_info("Session= {} Time_taken_to_remove_rawdatafields={}".format(session_identifier,(datetime.datetime.utcnow() - remove_rawdatafields_start_time_utc).seconds))

    helper.log_info("Session= {} Splunk Input Params received ingest_url={} tenant_id={} table_name={} table_mapping_name={} ingestion_mode={} remove_extra_fields={}".format(session_identifier,cluster_endpoint,tenant_id,table,database,mapping_name,durable_mode,remove_extra_fields))
    helper.log_info("Session= {} Alert action send_to_adx started...".format(session_identifier))

    adx_client = create_ADX_client(cluster_endpoint, app_id, app_secret, tenant_id, database, table, mapping_name, durable_mode, session_identifier)

    adx_client.send(helper, list(events))

    helper.log_info("Session= {} Total_Events_Successfully_sent = {}".format(session_identifier,adx_client.successfully_sent_events_number ))
    helper.log_info("Session= {} Script_Start_Time = {} Script_End_Time = {} Total_Time_Taken_Minutes = {} ".format(session_identifier, script_start_time_utc,datetime.datetime.utcnow(), ((datetime.datetime.utcnow() - script_start_time_utc ).seconds)/60  ))

    return 0