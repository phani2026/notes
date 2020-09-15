import json
import time
import traceback
import uuid
from pipelineblocksdk.api import BBSDK as sdk
from pipelineblocksdk.api.Singleton import Singleton
from pipelineblocksdk.construct.constants.MetricConfig import MetricConfig
import gc
import requests

from hdfs import InsecureClient
from hdfs.ext.kerberos import KerberosClient

from pipelineblocksdk.util.kerbUtil import generate_ticket_granting_ticket

import os
import threading
import logging
from shutil import rmtree


class ReadStream(Singleton):
    logger = None
    df_tmp_fil = None
    dtnct = None
    metrics_handler = None
    metrics = dict()
    localtime = time.localtime(time.time())
    # Possible values= COMPLETED, PARTIALLY_COMPLETED, FAILED
    block_status = "COMPLETED"
    left_df = None
    right_df = None
    resultant_join_df = None
    field_list = None
    list_of_struct_fields = None
    # Possible values= COMPLETED, PARTIALLY_COMPLETED, FAILED
    block_status = "COMPLETED"
    total_records = 0
    blockErrorInfo = {}
    blockProcessInfo = {}
    data_frame = None
    data_target_params = {}

    class ReadRecords(threading.Thread):
        def __init__(self, topic, idx, input_dict=None, block_params=None, optional_arg=None,
                     field_list=None, logger=None):
            threading.Thread.__init__(self)
            self.topic = topic
            self.idx = str(idx)
            self.input_dict = input_dict
            self.block_params = block_params
            self.optional_arg = optional_arg
            self.field_list = field_list
            if logger is None:
                self.logger = logging
            else:
                self.logger = logger

        def run(self):
            try:
                optional_arg = self.optional_arg
                self.logger.info('Started reading topic ' + self.idx)
                channels = self.optional_arg["channels"]
                read_msgs_channel = {
                    "channelId": channels[self.idx],
                    "registerId": ""
                }

                while True:
                    converted_list = []
                    self.logger.info('Reading....')
                    read_msgs_res = optional_arg['api_instance'].read_messages_from_topic_using_post(read_msgs_channel)
                    msgs = read_msgs_res.result
                    self.logger.info('messages len: ' + str(len(msgs)))
                    if len(msgs) == 0:
                        self.logger.info('Zero Messages')
                        topic = {"topicName": self.topic}
                        res = optional_arg['api_instance'].get_producer_status_using_post(topic)
                        self.logger.info('Zero messages: ' + json.dumps(res.result))
                        if not res.result['value']:
                            break

                    for msg in msgs:
                        msg = json.loads(msg)
                        converted_list.append(msg)

                    if len(converted_list) > 100000:
                        self.logger.info(
                            'Writing to file: ' + '/bigbrain/' + self.idx + ' len: ' + str(len(converted_list)))
                        converted_list.clear()


                self.logger.error('Done with ', self.idx)
                optional_arg['api_instance'].close_consumer_list_using_post(channels[self.idx])
            except Exception as e:
                self.logger.error(e)

    def get_client(self, block_params=None, connection_params=None):
        try:
            kerb_auth = False
            method = "https"

            if "https" in connection_params:
                if connection_params["https"]:
                    method = "https"
                else:
                    method = "http"

            host_name = connection_params["hostName"]
            port = connection_params["port"]

            if 'kerberos' in connection_params:
                kerb_auth = bool(connection_params['kerberos'])

            if kerb_auth:
                principal = generate_ticket_granting_ticket(block_params, connection_params["authName"])
                session = requests.Session()
                session.verify = False
                full_host = "%s://%s:%s" % (method, host_name, port)
                client = KerberosClient(url=full_host, session=session, mutual_auth='OPTIONAL', principal=principal)
                client.list('/')
                return client
            else:
                hadoop_host = host_name + ":" + port
                client = InsecureClient("http://" + hadoop_host)
                client.list('/')
                return client
        except Exception as e:
            self.logger.error("Error Occurred While Connecting to HDFS With Given Connection Details")
            raise e

    def validate_target_params(self):
        if ('filePath' not in self.data_target_params) or (self.data_target_params['filePath'] is None) or (
                self.data_target_params['filePath'] == ''):
            raise Exception('File path cannot be null or empty')
        if ('delimiter' not in self.data_target_params) or (self.data_target_params['delimiter'] is None):
            self.data_target_params['delimiter'] = ','
        if ('overwrite' not in self.data_target_params) or (self.data_target_params['overwrite'] is None):
            self.data_target_params['overwrite'] = True

    def validate_hdfs_connection(self, input_dict=None, block_params=None):
        connection_params = input_dict["ConnectionParams"]
        if ('hostName' not in connection_params) or (connection_params['hostName'] is None) or (
                connection_params['hostName'] == ''):
            raise Exception('HDFS Host URL cannot be null or empty')
        if ('port' not in connection_params) or (connection_params['port'] is None) or (
                connection_params['port'] == ''):
            raise Exception('HDFS Port cannot be null or empty')
        if ("kerberos" in connection_params):
            if "authName" not in connection_params:
                raise Exception("Please provide authName in Connection Parameters for Using Kerberos Authentication")
        client = self.get_client(block_params=block_params, connection_params=connection_params)
        return client

    def file_exits(self, hdfs_connection):
        self.logger.debug("Inside the file exists check method")
        try:
            return hdfs_connection.status(hdfs_path=self.data_target_params['fileWithFullPath'], strict=True)
        except Exception:
            return False

    def read_in_chunks(self, file_object, chunk_size=1024):
        """Lazy function (generator) to read a file piece by piece.
        Default chunk size: 1k."""
        while True:
            data = file_object.read(chunk_size)
            if not data:
                break
            yield data

    def block_write(self, hdfs_connection, file_path):
        self.logger.debug("Inside the write method")
        append = False
        with open(file_path) as f:
            for piece in self.read_in_chunks(f):
                with hdfs_connection.write(self.data_target_params['fileWithFullPath'],
                                           overwrite=self.data_target_params['overwrite'],
                                           buffersize=2048, append=append, encoding="utf-8") as writer:
                    writer.write(piece)
                    writer.flush()
                    append = True

        if self.data_target_params['overwrite']:
            self.data_target_params['overwrite'] = False

    @staticmethod
    def get_df_schema(df):
        try:
            json_schema = df.schema.json()
            json_schema = json.loads(json_schema)

            b2s_dict = {}

            for i, val in enumerate(json_schema['fields']):
                if val['type'] == 'integer':
                    b2s_dict[val['name'].upper()] = {'order': i + 1, 'active': True, 'type': 'IntegerType()'}
                if val['type'] == 'string':
                    b2s_dict[val['name'].upper()] = {'order': i + 1, 'active': True, 'type': 'StringType()'}
                if val['type'] == 'float' or val['type'] == 'double':
                    b2s_dict[val['name'].upper()] = {'order': i + 1, 'active': True, 'type': 'FloatType()'}
            return b2s_dict
        except Exception as e:
            raise e

    def init_log_and_metric_handlers(self, block_params=None):
        self.logger = sdk.block_log_handler(block_params)
        self.metrics_handler = sdk.metrics_api()
        self.metrics["appId"] = MetricConfig.GRAPH_APP_ID
        self.metrics["keys"] = block_params
        return

    def init(self, input_dict=None, block_params=None, program_arguments=None):
        try:
            self.init_log_and_metric_handlers(block_params)
            self.logger.info('Read stream block')
            self.logger.info('Input params:')
            self.logger.info(json.dumps(input_dict, sort_keys=True, indent=2))
            sdk.resource_monitor_handler(block_params).post_cpu_mem_usage()
            return
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.block_status = "FAILED"
            raise e

    def processMessagesForUi(self, process_expression):
        info = []
        error = []
        for index in self.blockProcessInfo:
            expression = process_expression[index]["expression"]
            if self.blockProcessInfo[index]["noOfLines"] > 0:
                info.append("Created column '" + self.blockProcessInfo[index][
                    "column_name"] + "' with expression [" + expression + "] applied for " + str(
                    self.blockProcessInfo[index]["noOfLines"]) + " records")
            if self.blockErrorInfo[index]["noOfErrorLines"] > 0:
                error.append(self.blockErrorInfo[index]["missingColumns"])
                error.append("Creation of column '" + self.blockProcessInfo[index][
                    "column_name"] + "' with expression [" + expression + "] failed on " + str(
                    self.blockErrorInfo[index]["noOfErrorLines"]) + " records")
        return info, error

    def run(self, input_dict=None, block_params=None, program_arguments=None):
        try:
            t1 = time.time()
            output_dict = dict()

            kafka_handler = sdk.kafka_handler(None)
            kafka_api_instance = kafka_handler.get_api_instance()
            channels = {}

            for count in range(5):
                self.logger.info("For Count: "+str(count))
                for key, data in input_dict.items():
                    topic = data['queueTopicName']
                    if not topic:
                        continue
                    print('queueTopicName: '+topic)
                    consumer_pool = {
                        "count": 1000,
                        "bufferSizeInMB": 1,
                        "groupId": str(uuid.uuid4()),
                        "registerId": "",
                        "topicsListToSubscribe": [
                            topic
                        ]
                    }

                    try:
                        consumer_pool_res = kafka_api_instance.create_consumer_list_using_post(consumer_pool)
                        channels[key] = consumer_pool_res.result
                    except Exception as e:
                        self.logger.error("Error Trying To Create a Consumer Of Topic:" + str(topic))
                        self.block_status = "FAILED"
                        raise e

                optional_param = {}
                optional_param["api_instance"] = kafka_api_instance
                optional_param["channels"] = channels


                self.field_list = {}

                arrary_of_threads = []
                for key, data in input_dict.items():
                    topic = data['queueTopicName']
                    if not topic:
                        continue
                    req = {"topicName": topic}
                    try:
                        schema = kafka_api_instance.get_topic_meta_using_post(req)
                        schema = json.loads(json.loads(schema.result)["schema"])
                        optional_param['schema'] = schema
                        self.logger.debug("Schema Received")
                    except Exception as e:
                        self.logger.error("Error Fetching Schema")
                        self.logger.error(str(e))
                        self.logger.error(traceback.format_exc())
                        self.block_status = "FAILED"
                        raise e

                    col_names = schema.keys()
                    parsed_schema_dict = {}

                    for name in col_names:
                        values = schema.get(name)
                        parsed_schema_dict[name] = values['type']

                    self.logger.info("schema")
                    self.logger.info(schema)
                    self.logger.info(parsed_schema_dict)

                    self.list_of_struct_fields = []
                    self.field_list[key] = []

                    fpath = '/bigbrain/' + str(key)
                    if os.path.exists(fpath):
                        rmtree(fpath)
                    os.makedirs(fpath, exist_ok=True)
                    t = self.ReadRecords(topic, key, input_dict, block_params,
                                         optional_param, self.field_list)
                    t.start()
                    t.join()
                    # arrary_of_threads.append(t)

                # for t in arrary_of_threads:
                #     t.join()

                print('All topics read done')

            self.logger.info("Output:")
            self.logger.info(json.dumps(output_dict, indent=2))

            output_dict["queueTopicName"] = ""
            output_dict['readerInfo'] = None
            output_dict['readerInfoError'] = None
            output_dict["infoKeys"] = None

            self.logger.info("Output:")
            self.logger.info(json.dumps(output_dict, indent=2))

            return output_dict

        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.block_status = "FAILED"
            raise e


bb_object = ReadStream()
