import json, os, sys, time, traceback, threading, logging
from collections import OrderedDict
from pipelineblocksdk.util.async_util import async
from pipelineblocksdk.construct.base.StreamBlock import StreamBlock
from pipelineblocksdk.api.Singleton import Singleton


class ReadBlock(StreamBlock, Singleton):
    logger = None
    metrics_handler = None
    metrics = dict()
    localtime = time.localtime(time.time())
    # Possible values= COMPLETED, PARTIALLY_COMPLETED, FAILED
    block_status = "COMPLETED"

    erroInfo = dict()
    readerInfo = dict()
    recordsRead = 0
    erroInfo["readerInfoError"] = dict()
    erroInfo["errorRecords"] = []
    schema = OrderedDict()
    total_count = 0

    class ReadRecords(threading.Thread):
        def __init__(self, topic, idx, input_dict=None, block_params=None, bb=None, logger=None):
            threading.Thread.__init__(self)
            self.topic = topic
            self.idx = str(idx)
            self.input_dict = input_dict
            self.block_params = block_params
            self.bb = bb
            self.consumer = bb.data_handler.create_consumer(topic_name=topic)
            self.input_schema = bb.get_schema(topic)
            self.block_start_time = time.time()
            if logger is None:
                self.logger = logging
            else:
                self.logger = logger

        def run(self):
            try:
                index_column_map = dict()
                delimiter = ","

                for c in self.input_schema.keys():
                    if "active" not in self.input_schema[c]:
                        index_column_map[self.input_schema[c]['order']] = c
                        continue
                    if self.input_schema[c]['active']:
                        index_column_map[self.input_schema[c]['order']] = c

                header_list = []

                for key in index_column_map:
                    col = index_column_map[key]
                    if delimiter in col:
                        col = '"' + col + '"'
                    header_list.append(col)

                batch_size = 10000
                line_count = 0
                batch_no = 1
                header_length = len(header_list)
                for line in self.consumer.receive():
                    if line:
                        line_count += 1
                        output_row = []
                        difference = [''] * (header_length - len(line))
                        line.extend(difference)
                        line = delimiter.join(map(str, output_row)) + '\n'

                        if line_count % batch_size == 0:
                            self.logger.info(f"Processed batch: {batch_no}")
                            self.logger.info(f"Line processed so far: {line_count}")
                            batch_no += 1

                self.block_status = "COMPLETED"
                self.logger.info("Streaming Completed for :" + self.topic)
                self.logger.info("Total time: " + str(time.time() - self.block_start_time))
                self.logger.info("Processed: "+str(line_count)+" Lines")



            except Exception as e:
                self.logger.error(e)

    @async
    def stream(self):

        self.block_status = "COMPLETED"

    def run(self):
        try:
            self.logger.info('Read Stream')
            self.logger.info('Input params:')
            self.logger.info(json.dumps(self.input_dict, sort_keys=True, indent=2))
            output_dict = dict()
            self.block_start_time = time.time()

            try:
                for count in range(5):
                    self.logger.info("For Count: "+str(count))
                    for key, data in self.input_dict.items():
                        topic = data['queueTopicName']
                        t = self.ReadRecords(topic, key, self.input_dict, self.block_params, self)
                        t.start()
                        t.join()

                self.stream()

            except Exception as exception:
                self.logger.error(str(exception))
                self.logger.error(traceback.format_exc())
                output_dict["infoKeys"] = ["readerInfoError"]
                output_dict['queueTopicName'] = None
                output_dict['readerInfo'] = None
                self.block_status = "FAILED"
                self.erroInfo["readerInfoError"]["errorMessages"] = {"validate_input": traceback.format_exc()}
                output_dict.update(self.erroInfo)
                raise exception

            output_dict["writeInfo"] = dict()
            output_dict["writeInfo"]["noOfRecords"] = self.total_count
            output_dict["writeInfoError"] = None
            output_dict["status"] = 200
            output_dict["infoKeys"] = ["status", "writeInfoError", "writeInfo"]

            return output_dict
        except Exception as e:
            exc_info = sys.exc_info()
            traceback.print_exception(*exc_info)
            self.logger.error(traceback.format_exc())
            self.block_status = "FAILED"
            raise e

    def manage_context(self):  # code to manage spark context
        return

    def stop_context_listener(self):
        # code to listen to stop context listener
        return
        # code to listen to stop context listener


bb_object = ReadBlock()

