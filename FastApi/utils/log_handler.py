import os
import logging
import requests
import colorlog
from typing import Union
from inspect import stack
from datetime import datetime
from multiprocessing import Process

# http://127.0.0.1:8000
class CustomLogger(logging.Logger):

    def __init__(self, name, level=logging.NOTSET):
        super().__init__(name, level)
        self.url = None
        self.script_name = self.__get_calling_script_name()

    def set_url(self, value: str) -> None:
        self.url = value
    
    # override error send logs to the server
    def error(self, msg, *args, **kwargs):
        self.send_logs(msg)
        return super(CustomLogger, self).warning(msg, *args, **kwargs)
    
    def __send_request(self, script_name : str, log : dict) -> None:
        if isinstance(log, str): log = {"message": log}
        body = {
             "script_name": script_name,
             "timestamp": datetime.now().isoformat(),
             "data": log
        }
        _ = requests.post(f"{self.url}/logs", json=body)

    def send_logs(self, logs : Union[str, dict]) -> None:
        if not self.url: raise ValueError("The URL is not set.")
        process = Process(target=self.__send_request, args=(self.script_name, logs))
        process.start()

    def __get_calling_script_name(self) -> str:
        # Get the stack frames
        stack_ = stack()
        # The calling script is the second frame in the stack
        calling_frame = stack_[-1]
        # Get the filename from the frame info
        script_name = os.path.basename(calling_frame.filename)
        return script_name.split(".")[0]
    
    def configure_handlers(self, log_directory: str) -> None:
        text_format = '%(log_color)s%(asctime)s | %(module)s:%(funcName)s | %(levelname)s | %(lineno)d |%(message)s '
        file_format = '%(asctime)s | %(module)s:%(funcName)s | %(levelname)s | %(lineno)d |%(message)s '

        os.makedirs(log_directory, exist_ok=True)

        file_handler = logging.FileHandler(f'{log_directory}/main.log')
        file_handler.setFormatter(logging.Formatter(file_format))
        file_handler.setLevel(logging.DEBUG)
        self.addHandler(file_handler)

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        console_handler.setFormatter(colorlog.ColoredFormatter(text_format))
        self.addHandler(console_handler)