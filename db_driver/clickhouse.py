import traceback
from . import Driver
from log import logger
from configparser import ConfigParser
from clickhouse_driver import connect


class ClickhouseDriver(Driver):
    def __init__(self):
        config = ConfigParser()
        config.read("./config.ini")
        host = config.get("clickhouse", "host")
        port = int(config.get("clickhouse", "port"))
        user = config.get("clickhouse", "user")
        password = config.get("clickhouse", "password")
        self.database = config.get("clickhouse", "database")
        self.clickhouse_connect = connect(host=host, port=port, database=self.database,
                                          user=user, password=password)
        self._cursor = self.clickhouse_connect.cursor()
        self._data = None

    def insert(self, query, data):
        try:
            if isinstance(data, tuple):
                self._cursor.execute(query, data)
                return self._cursor.rowcount
            elif isinstance(data, list):
                self._cursor.executemany(query, data)
                return self._cursor.rowcount
        except Exception as e:
            logger.error(traceback.format_exc())
            raise e

    def update(self, data):
        pass

    def delete(self, data):
        pass

    def query(self, query):
        try:
            self._cursor.execute(query)
            self._data = self._cursor.fetchall()
            return self._cursor.rowcount
        except Exception as e:
            logger.error(traceback.format_exc())
            raise e

    def get_result(self, offset=0):
        return self._data
