import logging
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import Iterable

import pandas as pd

class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]

def raise_arg(arg_name, arg_val, arg_list):
    if arg_val not in arg_list:
        raise ValueError("{} must be one in {}".format(arg_name, arg_list))

def create_file_log(
    name:str="test",
    level:logging=logging.DEBUG,
    format:str="%(asctime)s (%(levelname)s) %(message)s",
    abs_loc=""
):
    logger = logging.getLogger(name)
    logger.setLevel(level)

    stream_handler = logging.StreamHandler()
    file_handler = logging.FileHandler(filename=abs_loc+name+".log", encoding="utf-8")
    formatter = logging.Formatter(format)

    stream_handler.setLevel(level)
    stream_handler.setFormatter(formatter)
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)

    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)

    logger.propagate = False

    return logger

class DateManager:

    @staticmethod
    def now(format="%Y:%m:%d %H:%M:%S.%f"):
        return datetime.fromtimestamp(time.time()).strftime(format)

    @staticmethod
    def str_to_datetime(data, format="%Y-%m-%d"):
        if type(data) == str:
            return datetime.strptime(data, format)
        elif isinstance(data, Iterable):
            tmp_list = []
            for val in list(data):
                tmp_list.append(datetime.strptime(val))
            return tmp_list

    @staticmethod
    def pd_to_datetime(data, format="%Y-%m-%d"):
        if type(data) == pd._libs.tslibs.timestamps.Timestamp:
            return datetime.strptime(data.strftime("%Y-%m-%d"), format)
        elif isinstance(data, Iterable):
            tmp_list = []
            for val in list(data):
                tmp_list.append(val.strftime("%Y-%m-%d"))
            return tmp_list

    @staticmethod
    def datetime_to_pd(data, format="%Y-%m-%d"):
        if type(data) == datetime:
            return pd.to_datetime(data.strftime(format))
        elif isinstance(data, Iterable):
            tmp_list = []
            for val in list(data):
                tmp_list.append(val.strftime(format))
            return tmp_list

    @staticmethod
    def timestamp_to_datetime(data, format="%Y-%m-%d"):
        if type(data) == int:
            return datetime.fromtimestamp(int(data)).strftime(format) # %H:%M:%S
        elif isinstance(data, Iterable):
            tmp_list = []
            for val in list(data):
                tmp_list.append(datetime.fromtimestamp(int(val)).strftime(format))
            return tmp_list
    
    @staticmethod
    def str_to_timestamp(data):
        if type(data) == str:
            return int(time.mktime(pd.to_datetime(data).timetuple()))
        elif isinstance(data, Iterable):
            tmp_list = []
            for val in list(data):
                tmp_list.append(int(time.mktime(pd.to_datetime(val).timetuple())))
            return tmp_list    


    @staticmethod
    def split(start, end, format="%Y-%m-%d", backward=False, **kargs):
        start_date = DateManager.str_to_datetime(start, format)
        end_date = DateManager.str_to_datetime(end, format)

        if backward == False:
            tmp_list = [start_date.strftime(format)]
            while True:
                start_date = start_date + relativedelta(**kargs)
                if start_date < end_date:
                    tmp_list.append(start_date.strftime(format))
                else:
                    tmp_list.append(end_date.strftime(format))
                    break
        else:
            tmp_list = [end_date.strftime(format)]
            while True:
                end_date = end_date - relativedelta(**kargs)
                if start_date < end_date:
                    tmp_list.append(end_date.strftime(format))
                else:
                    tmp_list.append(start_date.strftime(format))
                    tmp_list = list(reversed(tmp_list))
                    break

        return tmp_list        