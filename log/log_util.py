import threading
from loguru import logger as lo_logger


class Logger:
    def __init__(self):
        self.logger = lo_logger

    def set_name(self, name=""):
        self.logger.add("/var/bigquery-extractor/log/{time:YYYYMMDD}_" + name + ".log",
                        format="[{level}]{time: YYYY-MM-DD HH:mm:ss} {message}",
                        level="INFO", rotation="00:00")
        self.logger.add("/var/bigquery-extractor/log/{time:YYYYMMDD}_" + name + ".log",
                        format="[{level}]{time: YYYY-MM-DD HH:mm:ss} {message}",
                        level="ERROR", rotation="00:00")

    def info(self, message):
        self.logger.info(message)

    def error(self, message):
        self.logger.error(message)


logger = Logger()
