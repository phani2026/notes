import json
import time
import traceback
import uuid
from pipelineblocksdk.api import BBSDK as sdk
from pipelineblocksdk.api.Singleton import Singleton
from pipelineblocksdk.construct.constants.MetricConfig import MetricConfig
from pipelineblocksdk.data.spark.SparkConfCustom import SparkConfCustom
import gc
import pyspark
import requests
from pyspark.sql.functions import udf
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, FloatType

from hdfs import InsecureClient
from hdfs.ext.kerberos import KerberosClient

from pipelineblocksdk.util.kerbUtil import generate_ticket_granting_ticket

import pyspark
import os
import threading
import logging
from shutil import rmtree


class SparkJoin(Singleton):
    logger = None
    df_tmp_fil = None
    dtnct = None
    metrics_handler = None
    metrics = dict()
    sqlDF = None
    localtime = time.localtime(time.time())
    # Possible values= COMPLETED, PARTIALLY_COMPLETED, FAILED
    block_status = "COMPLETED"
    spark_schema = None
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
        def __init__(self, spark, topic, idx, schema, input_dict=None, block_params=None, optional_arg=None,
                     field_list=None, logger=None):
            threading.Thread.__init__(self)
            self.spark = spark
            self.topic = topic
            self.idx = str(idx)
            self.schema = schema
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
                converted_list = []
                while True:
                    self.logger.info('Reading....')
                    read_msgs_res = optional_arg['api_instance'].read_messages_from_topic_using_post(read_msgs_channel)
                    msgs = read_msgs_res.result
                    self.logger.info('messages len' + str(len(msgs)))
                    if (len(msgs) == 0):
                        self.logger.info('Zero Messages')
                        topic = {"topicName": self.topic}
                        res = optional_arg['api_instance'].get_producer_status_using_post(topic)
                        self.logger.info('Zero messages: ' + json.dumps(res.result))
                        if not res.result['value']:
                            break

                    for msg in msgs:
                        msg = json.loads(msg)
                        for index, i in enumerate(msg):
                            if self.field_list[self.idx][index] == 'int':
                                msg[index] = int(float(msg[index]))
                            elif self.field_list[self.idx][index] == 'float':
                                msg[index] = float(msg[index])
                        converted_list.append(msg)

                    if len(converted_list) > 100000:
                        self.logger.info(
                            'Writing to file: ' + '/bigbrain/' + self.idx + ' len: ' + str(len(converted_list)))
                        df = self.spark.createDataFrame(converted_list, self.schema)
                        df.write.mode('append').parquet('/bigbrain/' + self.idx)
                        converted_list.clear()

                if len(converted_list) > 0:
                    self.logger.info('Writing to file', "/bigbrain/" + self.idx, str(len(converted_list)))
                    df = self.spark.createDataFrame(converted_list, self.schema)
                    df.write.mode('append').parquet('/bigbrain/' + self.idx)
                    converted_list.clear()

                self.logger.error('Done with ', self.idx)
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
            self.logger.info('Decision Tree Binary Classifier')
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

    def stream_block2(self, input_dict=None, block_params=None, optional_arg=None):
        try:
            self.data_frame = optional_arg['df_path']
            schema = optional_arg['schema']
            topic = optional_arg['topic_id']

            batch_size = input_dict['Config']['batchSize']

            records_wrote = []

            records_wrote.append(0)
            batch_no = 1

            self.logger.info(f"Started Writing Data Frame to Queue")

            repartition_col = input_dict['Config']['repartition_col']

            self.logger.info(f'finding distinct values of {repartition_col}')

            exec("self.dtnct = self.data_frame.select('" + repartition_col + "').distinct().collect()")

            # rdd_df = data_frame.rdd.zipWithIndex()
            # df_final = rdd_df.toDF()

            self.logger.info(f"found distinct keys")

            intial_val = 0
            for dstnct_vals in self.dtnct:
                val_fil = dstnct_vals[repartition_col]

                exec(
                    "self.df_tmp_fil = self.data_frame.filter(self.data_frame['" + repartition_col + "'] == " + val_fil + ")")

                self.logger.info(f"writing {batch_no}")
                batch_no += 1

                temp_messages = []
                for row in self.df_tmp_fil.collect():
                    row = list(row)
                    row = [str(x) for x in row]
                    temp_messages.append(json.dumps(row))

                publish_req = {
                    "messageList": temp_messages,
                    "registerId": "",
                    "topicName": topic
                }

                optional_arg["api_instance"].publish_messages_to_topic_using_post(publish_req)
                records_wrote[-1] += len(temp_messages)
                info, error = self.processMessagesForUi(records_wrote)
                ui_info = {"info": info, "error": error}

                topic_name = {
                    "name": topic,
                    "metaData": {
                        "schema": json.dumps(schema),
                        "readerInfo": json.dumps({
                            "noOfRecordsRead": records_wrote[-1]
                        }),

                        "ui_info": json.dumps(ui_info),
                        "total_messages": records_wrote[-1]
                    }
                }

                optional_arg["api_instance"].update_topic_meta_using_post(topic_name)
                self.logger.info(f"Written {records_wrote[-1]} records")
                gc.collect()

            self.logger.info("hbbbbubuuuuuuuuuu")

            # for row in df_rdd.collect():
            #     row = list(row)
            #     row = [str(x) for x in row]
            #     temp_messages.append(json.dumps(row))
            #
            #     if len(temp_messages) % batch_size == 0:
            #         batch_no += 1
            #
            #         publish_req = {
            #             "messageList": temp_messages,
            #             "registerId": "",
            #             "topicName": topic
            #         }
            #
            #         optional_arg["api_instance"].publish_messages_to_topic_using_post(publish_req)
            #         records_wrote[-1] += len(temp_messages)
            #
            #         info, error = self.processMessagesForUi(records_wrote)
            #         ui_info = {"info": info, "error": error}
            #
            #         topic_name = {
            #             "name": topic,
            #             "metaData": {
            #                 "schema": json.dumps(schema),
            #                 "readerInfo": json.dumps({
            #                     "noOfRecordsRead": records_wrote[-1]
            #                 }),
            #
            #                 "ui_info": json.dumps(ui_info),
            #                 "total_messages": records_wrote[-1]
            #             }
            #         }
            #
            #         optional_arg["api_instance"].update_topic_meta_using_post(topic_name)
            #         self.logger.info(f"Written {records_wrote[-1]} records")
            #         temp_messages = []

            # if (batch_no == 1) or temp_messages:
            #     publish_req = {
            #         "messageList": temp_messages,
            #         "registerId": "",
            #         "topicName": topic
            #     }
            #     optional_arg["api_instance"].publish_messages_to_topic_using_post(publish_req)
            #     records_wrote[-1] += len(temp_messages)
            #
            #     info, error = self.processMessagesForUi(records_wrote)
            #     ui_info = {"info": info, "error": error}
            #
            #     topic_name = {
            #         "name": topic,
            #         "metaData": {
            #             "schema": json.dumps(schema),
            #             "readerInfo": json.dumps({
            #                 "noOfRecordsRead": records_wrote[-1]
            #             }),
            #             "ui_info": json.dumps(ui_info),
            #             "total_messages": records_wrote[-1]
            #         }
            #     }
            #
            #     optional_arg["api_instance"].update_topic_meta_using_post(topic_name)
            #     self.logger.info(f"Written {records_wrote[-1]} records")

            release_request = {
                "topicName": topic
            }

            self.logger.debug(f"Releasing Producer For Topic: {topic}")
            optional_arg["api_instance"].release_producer_using_post(release_request)
            self.logger.debug(f"Producer Released")

            self.block_status = "COMPLETED"
            self.logger.info("Stream Complete")

        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.block_status = "FAILED"
            raise e

    def run(self, input_dict=None, block_params=None, program_arguments=None):
        try:
            t1 = time.time()
            output_dict = dict()

            configs = input_dict["Config"]
            # test_args = {'spark.app.name': 'spark_app_test', 'spark.shuffle.service.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '1', 'spark.dynamicAllocation.enabled': 'true'}

            queue_dict = {}

            queue_dict['left_df'] = input_dict['leftData']['queueTopicName']
            queue_dict['right_df'] = input_dict['rightData']['queueTopicName']

            kafka_handler = sdk.kafka_handler(None)
            kafka_api_instance = kafka_handler.get_api_instance()
            channels = {}

            for key, topic in queue_dict.items():
                consumer_pool = {
                    "count": 1,
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
            optional_param['queue_dict'] = queue_dict
            optional_param["api_instance"] = kafka_api_instance
            optional_param["channels"] = channels

            self.spark = SparkConfCustom().get_spark_session()
            self.spark.sparkContext.setLogLevel('ERROR')

            self.spark_schema = {}
            self.field_list = {}
            print('waiting')
            # time.sleep(200)

            arrary_of_threads = []
            for key, topic in queue_dict.items():
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

                self.logger.info("schemaaaa hereeee")
                self.logger.info(schema)
                self.logger.info(parsed_schema_dict)

                self.list_of_struct_fields = []
                self.field_list[key] = []

                for name in parsed_schema_dict.keys():
                    if parsed_schema_dict[name] == 'FloatType()':
                        self.field_list[key].append(('float'))
                        self.list_of_struct_fields.append(StructField(name, FloatType(), True))
                    elif parsed_schema_dict[name] == 'IntegerType()':
                        self.field_list[key].append('int')
                        self.list_of_struct_fields.append(StructField(name, IntegerType(), True))
                    elif parsed_schema_dict[name] == 'DoubleType()':
                        self.field_list[key].append('float')
                        self.list_of_struct_fields.append(StructField(name, DoubleType(), True))
                    else:
                        self.field_list[key].append('string')
                        self.list_of_struct_fields.append(StructField(name, StringType(), True))

                self.spark_schema[key] = StructType(self.list_of_struct_fields)
                fpath = '/bigbrain/' + str(key)
                if os.path.exists(fpath):
                    rmtree(fpath)
                os.makedirs(fpath, exist_ok=True)
                t = self.ReadRecords(self.spark, topic, key, self.spark_schema[key], input_dict, block_params,
                                     optional_param, self.field_list)
                t.start()
                arrary_of_threads.append(t)

            for t in arrary_of_threads:
                t.join()

            print('Both topics read done')

            # self.stream_block(input_dict=input_dict, block_params=block_params, optional_arg=optional_param)

            self.left_df = self.spark.read.parquet('/bigbrain/left_df')
            print(self.left_df.count())
            self.right_df = self.spark.read.parquet('/bigbrain/right_df')
            print(self.right_df.count())
            exec("self.resultant_join_df" + "=" + "self.left_df.join(self.right_df,self.left_df['" + configs[
                'unique_key_left'] + "']== self.right_df['" + configs['unique_key_right'] + "'] ,how='" + configs[
                     'join_type'] + "')")

            print(self.left_df.rdd.getNumPartitions())
            print(self.right_df.rdd.getNumPartitions())

            new_column_name_list = self.resultant_join_df.columns
            renamed_cols = {}
            for col in new_column_name_list:
                count = new_column_name_list.count(col)
                if count > 1:
                    idx = new_column_name_list.index(col)
                    new_column_name_list[idx] = 'bbl_' + col

            print(self.resultant_join_df.columns)
            self.resultant_join_df = self.resultant_join_df.toDF(*new_column_name_list)
            print(self.resultant_join_df.columns)

            self.resultant_join_df.write.csv('/tmp/test')
            print(self.resultant_join_df.rdd.getNumPartitions())

            self.logger.info("*****************************")
            self.logger.info("Join Completed")

            # create topic for the result to be stored
            kafka_handler = sdk.kafka_handler(None)
            api_instance = kafka_handler.get_api_instance()

            # preprocess and detect schema, add meta
            block_start_time = time.time()

            schema_new = self.get_df_schema(self.resultant_join_df)

            operationalParams = {}
            operationalParams["api_instance"] = api_instance
            operationalParams["block_start_time"] = block_start_time
            operationalParams["dataframe_name"] = "resultant_join_df"
            operationalParams["data_frame"] = self.resultant_join_df
            operationalParams["schema"] = schema_new

            try:
                resultant_topic = str(uuid.uuid4())

                topic = {
                    "name": resultant_topic,
                    "identifiers": block_params,
                    "displayName": 'resultantQueueTopic'
                }
                topic_res = api_instance.create_topic_using_post(topic)

                topic_name = {
                    "name": resultant_topic,
                    "metaData": {
                        "schema": json.dumps(schema_new)
                    }
                }
                api_instance.update_topic_meta_using_post(topic_name)

                self.logger.info(f"Schema Added For Topic {resultant_topic}")

            except Exception as e:
                self.logger.error("Error creating resultant topic " + str(resultant_topic))
                raise e

            operationalParams["topic_id"] = resultant_topic

            if input_dict['Config']['streamOutput']:
                self.stream_block2(input_dict=input_dict, optional_arg=operationalParams, block_params=block_params)
            else:
                self.data_target_params = input_dict["DataTarget"]
                try:
                    self.validate_target_params()
                except Exception as e:
                    self.logger.error(str(e))
                    raise e

                try:
                    self.client = self.validate_hdfs_connection(input_dict=input_dict, block_params=block_params)
                    self.data_target_params['fileWithFullPath'] = self.data_target_params['filePath']
                    if not self.data_target_params['overwrite']:
                        exists = self.file_exits(hdfs_connection=self.client)
                        if exists:
                            raise FileExistsError("File Already Exists: " + str(self.data_target_params['filePath']))
                except Exception as e:
                    self.logger.error(str(e))
                    raise e

                self.block_write(self.client, '/tmp/test')

            self.logger.info("Output:")
            self.logger.info(json.dumps(output_dict, indent=2))

            output_dict["queueTopicName"] = resultant_topic
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


bb_object = SparkJoin()
