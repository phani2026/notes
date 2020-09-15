import json
import time
import traceback
import uuid
from pipelineblocksdk.api import BBSDK as sdk
from pipelineblocksdk.api.Singleton import Singleton
from pipelineblocksdk.construct.constants.MetricConfig import MetricConfig
from pipelineblocksdk.data.spark.SparkConfCustom import SparkConfCustom
from pipelineblocksdk.util.ThirdPartyIntegration import get_oracle_creds

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


class SparkJoinOracle(Singleton):
    logger = None
    metrics_handler = None
    metrics = dict()
    client = None
    localtime = time.localtime(time.time())
    # Possible values= COMPLETED, PARTIALLY_COMPLETED, FAILED
    block_status = "COMPLETED"
    spark_schema = None
    left_df = None
    right_df = None
    resultant_join_df = None
    field_list = None
    total_records = 0
    blockErrorInfo = {}
    blockProcessInfo = {}
    data_target_params = {}
    data_source_one = {}
    data_source_two = {}
    append = False
    temp_file_paths = []

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

    def delete_file(self, hdfs_connection):
        self.logger.debug("Inside delete HDFS file method")
        try:
            return hdfs_connection.delete(self.data_target_params['fileWithFullPath'])
        except Exception:
            return False

    def read_in_chunks(self, file_object, chunk_size=2048000):
        """Lazy function (generator) to read a file piece by piece.
        Default chunk size: 1k."""
        while True:
            data = file_object.read(chunk_size)
            if not data:
                break
            yield data

    def block_write(self, hdfs_connection, file_path):
        self.logger.info("Inside the write method")
        with open(file_path) as f:
            for piece in self.read_in_chunks(f):
                with hdfs_connection.write(self.data_target_params['fileWithFullPath'],
                                           buffersize=604800, append=self.append, encoding="utf-8") as writer:
                    writer.write(piece)
                    writer.flush()
                    self.append = True
        self.logger.info("Done writing")

    def block_line_write(self, hdfs_connection, file_path):
        self.logger.info("Inside the write method")
        with open(file_path) as reader, hdfs_connection.write(self.data_target_params['fileWithFullPath'],
                                                              append=self.append, encoding="utf-8") as writer:
            for line in reader:
                writer.write(line)
        self.append = True
        self.logger.info("Done writing")

    def block_folder_write(self, hdfs_connection, file_path):
        self.logger.info("Inside the write method")
        hdfs_connection.upload(self.data_target_params['fileWithFullPath'], file_path, n_threads=-1,
                               chunk_size=5000000, cleanup=True, overwrite=self.data_target_params['overwrite'])
        self.logger.info("Done writing")

    def getOracleDF(self, spark_session, data_source, block_params):
        self.logger.info("Inside the oracle df method")
        credentials = get_oracle_creds(user_id=block_params["userAuthToken"],
                                       authentication_name=data_source['connection_name'])
        url = "jdbc:oracle:thin:@%s:%s:%s" % (credentials["host"], credentials["port"], credentials["sid"])
        return spark_session.read.format("jdbc").options(url=url
                                                         , driver="oracle.jdbc.driver.OracleDriver"
                                                         , dbtable=data_source['query']
                                                         , numPartitions=data_source['num_partitions']
                                                         , lowerBound=0
                                                         , upperBound=1000000
                                                         , partitionColumn=data_source['partition_column']
                                                         , user=credentials["username"]
                                                         , password=credentials["password"]).load()

    def getHDFSDF(self, spark_session, data_source, block_params, hdfs_connection):
        self.logger.info("Inside the hdfs df method")
        localFilePath = '/bigbrain/temp_files/' + str(time.time()) + '.csv'
        hdfs_connection.download(data_source['fileWithFullPath'], localFilePath, n_threads=-1,
                                 chunk_size=5000000, overwrite=True)
        self.logger.info("Read HDFS file to local")
        self.temp_file_paths.append(localFilePath)
        return spark_session.read.format("csv").option("header", data_source['header']).options("delimiter",
                                                                                                data_source[
                                                                                                    'delimiter']).load(
            localFilePath)

    def getDF(self, spark_session, data_source, block_params):
        self.logger.info("Inside the oracle df method")
        if data_source['type'] == 'hdfs':
            return self.getHDFSDF(spark_session, data_source, block_params)
        else:
            return self.getOracleDF(spark_session, data_source, block_params)

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

    def run(self, input_dict=None, block_params=None, program_arguments=None):
        try:
            t1 = time.time()
            output_dict = dict()

            configs = input_dict["Config"]
            # test_args = {'spark.app.name': 'spark_app_test', 'spark.shuffle.service.enabled': 'true', 'spark.dynamicAllocation.minExecutors': '1', 'spark.dynamicAllocation.enabled': 'true'}

            self.spark = SparkConfCustom().get_spark_session()
            self.spark.sparkContext.setLogLevel('ERROR')

            try:
                self.validate_target_params()
                self.client = self.validate_hdfs_connection(input_dict=input_dict, block_params=block_params)
            except Exception as e:
                self.logger.error(str(e))
                raise e

            self.spark_schema = {}
            self.field_list = {}

            self.left_df = self.getDF(self.spark, input_dict['ds1'], block_params)
            print(self.left_df.count())
            self.right_df = self.getDF(self.spark, input_dict['ds2'], block_params)
            print(self.right_df.count())
            exec("self.resultant_join_df" + "=" + "self.left_df.join(self.right_df,self.left_df['" + configs[
                'left_col'] + "']== self.right_df['" + configs['right_col'] + "'] ,how='" + configs[
                     'join_type'] + "')")

            print(self.left_df.rdd.getNumPartitions())
            print(self.right_df.rdd.getNumPartitions())

            new_column_name_list = self.resultant_join_df.columns
            for col in new_column_name_list:
                count = new_column_name_list.count(col)
                if count > 1:
                    idx = new_column_name_list.index(col)
                    new_column_name_list[idx] = col + '_1'

            print(self.resultant_join_df.columns)
            self.resultant_join_df = self.resultant_join_df.toDF(*new_column_name_list)
            print(self.resultant_join_df.columns)

            temp_fp = '/bigbrain/temp_file/' + str(t1) + '.csv'
            self.temp_file_paths.append(temp_fp)
            print(temp_fp)
            # os.makedirs(temp_fp, exist_ok=True)
            temp_join_time_st = time.time()
            self.resultant_join_df.write.mode("overwrite").option("header", "true").csv(temp_fp)
            temp_join_time_end = time.time()
            print('Time for Join: ' + str(temp_join_time_end - temp_join_time_st) + ', File Partitions' + str(
                self.resultant_join_df.rdd.getNumPartitions()))

            self.logger.info("*****************************")
            self.logger.info("Join Completed")
            # self.logger.info("Count: " + str(self.resultant_join_df.count()))

            self.data_target_params = input_dict["DataTarget"]

            try:
                self.data_target_params['fileWithFullPath'] = self.data_target_params['filePath']
                exists = self.file_exits(hdfs_connection=self.client)
                if exists:
                    if self.data_target_params['overwrite']:
                        # remove file
                        self.delete_file(hdfs_connection=self.client)
                        self.append = False
                    else:
                        raise FileExistsError("File Already Exists: " + str(self.data_target_params['filePath']))
            except Exception as e:
                self.logger.error(str(e))
                raise e

            write_start_time = time.time()
            self.logger.info("Writing to HDFS:")
            self.block_folder_write(self.client, temp_fp)
            # if os.path.isdir(temp_fp):
            #     self.logger.info("dir")
            #     for filename in os.listdir(temp_fp):
            #         print(filename)
            #         if filename.endswith(".csv"):
            #             csv_path = temp_fp + '/' + filename
            #             print(csv_path)
            #             self.block_line_write(self.client, csv_path)
            # else:
            #     self.block_line_write(self.client, temp_fp)

            print('Time for Join: ' + str(temp_join_time_end - temp_join_time_st))

            self.logger.info("Time taken to write to HDFS: " + str(time.time() - write_start_time))

            self.logger.info("Output:")
            self.logger.info(json.dumps(output_dict, indent=2))

            output_dict["queueTopicName"] = ''
            output_dict['readerInfo'] = None
            output_dict['readerInfoError'] = None
            output_dict["infoKeys"] = None

            self.logger.info("Cleaning temp files:")
            for file in self.temp_file_paths:
                rmtree(path=file, ignore_errors=True)


            self.logger.info("Output:")
            self.logger.info(json.dumps(output_dict, indent=2))

            return output_dict

        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.block_status = "FAILED"
            raise e


bb_object = SparkJoinOracle()

# ///
# ds1
# {
#     "connection_name": "myOracle",
#     "query": "(select * from CUSTOMERS)",
#     "query": "(select * from CUSTOMERS)",
#     "query": "(select * from CUSTOMERS)",
# }
#
# Config
# {
#     "left_col": "CUSTOMER_NAME",
#     "right_col": "CUSTOMER_NAME",
#     "join_type": "left_outer"
# }
#
# //ConnectionParams
# {
#     "hostName": "betabb.razorthink.net",
#     "port": "50070",
#     "kerberos": false,
#     "authName": "",
#     "https": false
# }


# ds2
# {
#     "connection_name": "myOracle",
#     "query": "(select * from CUSTOMERS)",
#     "num_partitions": 1,
#     "partition_column": "CUSTOMER_NAME"
# }
#
# DataTarget
#
# {
#     "filePath": "/data/hdfs-nfs/Data/6d72d2cf-45b6-464c-9811-d6cf21574ece/test_oracle_hdfs_phani.csv",
#     "delimiter": ",",
#     "overwrite": true
# }
