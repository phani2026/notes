import logging
import uuid
from threading import Thread
from multiprocessing import Process


def write_logs_in_process():
    logging.info("in process info")
    logging.error("in process error")
    print('in process print', uuid.uuid4())

def write_logs_in_thread():
    logging.info("in thread info")
    logging.error("in thread error")
    print('in thread print', uuid.uuid4())

def write_logs():
    logging.info("hello info")
    logging.debug("hello debug")
    logging.error("hello error")
    print('hello print', uuid.uuid4())

    thread = Thread(target=write_logs_in_thread, daemon=True)
    thread.start()

    process = Process(target=write_logs_in_thread, daemon=True)
    process.start()






