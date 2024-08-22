
import logging
import threading
import datetime
from concurrent.futures import ThreadPoolExecutor

import _env

def init_threadpool(numb_thread):
    return ThreadPoolExecutor(max_workers=numb_thread)

def get_thread_name():
    return threading.current_thread().getName()
