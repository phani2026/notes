import json
import time
import traceback
from pipelineblocksdk.api import BBSDK as sdk
from pipelineblocksdk.api.Singleton import Singleton
from pipelineblocksdk.construct.constants.MetricConfig import MetricConfig
import requests

from hdfs import InsecureClient
from hdfs.ext.kerberos import KerberosClient

from pipelineblocksdk.util.kerbUtil import generate_ticket_granting_ticket

import os
from shutil import rmtree


class CSVWriter(Singleton):
    logger = None
    metrics_handler = None
    metrics = dict()
    client = None
    sqlDF = None
    localtime = time.localtime(time.time())
    # Possible values= COMPLETED, PARTIALLY_COMPLETED, FAILED
    block_status = "COMPLETED"
    spark_schema = None
    field_list = None
    list_of_struct_fields = None
    # Possible values= COMPLETED, PARTIALLY_COMPLETED, FAILED
    block_status = "COMPLETED"
    total_records = 0
    blockErrorInfo = {}
    blockProcessInfo = {}
    data_frame = None
    data_target_params = {}
    append = False

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

    def run(self, input_dict=None, block_params=None, program_arguments=None):
        try:
            t1 = time.time()
            output_dict = dict()

            configs = input_dict["Config"]

            temp_fp = '/bigbrain/' + str(t1) + '.csv'

            self.data_target_params = input_dict["DataTarget"]
            try:
                self.validate_target_params()
            except Exception as e:
                self.logger.error(str(e))
                raise e

            try:
                self.client = self.validate_hdfs_connection(input_dict=input_dict, block_params=block_params)
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
            if os.path.isdir(temp_fp):
                self.logger.info("dir")
                for filename in os.listdir(temp_fp):
                    print(filename)
                    if filename.endswith(".csv"):
                        csv_path = temp_fp + '/' + filename
                        print(csv_path)
                        self.block_line_write(self.client, csv_path)
            else:
                self.block_line_write(self.client, temp_fp)

            self.logger.info("Time taken to write to HDFS: " + str(time.time() - write_start_time))

            self.logger.info("Output:")
            self.logger.info(json.dumps(output_dict, indent=2))

            output_dict["queueTopicName"] = ''
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


bb_object = CSVWriter()
