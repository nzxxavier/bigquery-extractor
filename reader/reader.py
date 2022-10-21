import importlib
import os
from log import logger
from configparser import ConfigParser
from db_driver import BigqueryDriver
from utils import format_data
from google.api_core.exceptions import NotFound
from datetime import datetime, timedelta


BASE_DIR = '/var/bigquery-extractor'


class Reader:
    def __init__(self, name,  mode, day_offset=0, hour_offset=4):
        self.name = name
        self._config = ConfigParser()
        self._config.read(f"{BASE_DIR}/temp/config.ini")
        destination = self._config.get("datasource", "destination")
        destination_driver_module = importlib.import_module("db_driver")
        if destination == "clickhouse":
            destination_driver_class = getattr(destination_driver_module, "ClickhouseDriver")
        else:
            destination_driver_class = None
        self._destination_driver = destination_driver_class()
        self._bigquery_driver = BigqueryDriver()
        self._destination_db = self._config.get(destination, "database")
        self._destination_table = self._config.get(destination, "table")
        self._target_project = self._config.get("bigquery", "project")
        self._target_dataset = self._config.get("bigquery", "dataset_id")
        self._target_table_prefix = self._config.get("bigquery", "table_prefix")
        self._day = (datetime.now() - timedelta(days=day_offset)).strftime("%Y%m%d")
        if mode == 'minute':
            self._init_timestamp = int(datetime.timestamp(datetime.now() - timedelta(hours=1)) * 1000000)
        elif mode == 'hour':
            self._init_timestamp = int(datetime.timestamp(datetime.now() - timedelta(hours=hour_offset)) * 1000000)
        else:
            self._init_timestamp = 0

    def read(self):
        last_timestamp = self.get_last_timestamp()
        select_query = "SELECT  * " \
                       f"FROM    `{self._target_project}.{self._target_dataset}.{self._target_table_prefix}_{self._day}` " \
                       f"WHERE    event_timestamp > {last_timestamp} " \
                       f"ORDER BY event_timestamp ASC"
        logger.info(f"select_query: {select_query}")
        insert_query = f"insert into {self._destination_db}.{self._destination_table} (event_date, event_timestamp, event_name, event_params, " \
                        "event_previous_timestamp, event_value_in_usd, " \
                        "event_bundle_sequence_id, event_server_timestamp_offset, " \
                        "user_id, user_pseudo_id, privacy_info, user_properties, " \
                        "user_first_touch_timestamp, user_ltv, device, geo, app_info, " \
                        "traffic_source, stream_id, platform, event_dimensions, " \
                        "ecommerce, items, update_time) values"
        logger.info(f"insert_query: {insert_query}")
        logger.info(f"init timestamp: {self._init_timestamp}")
        logger.info(f"real timestamp: {last_timestamp}")
        try:
            self._bigquery_driver.query(select_query)
            result = self._bigquery_driver.get_result()
            row_count = 0
            for page in result:
                current_timestamp = 0
                rows = page.to_pylist()
                data = []
                for row in rows:
                    current_timestamp = max(current_timestamp, row['event_timestamp'])
                    data.append(format_data(row))
                row_count += rows.__len__()
                self._destination_driver.insert(insert_query, data)
                last_timestamp = current_timestamp
                self.set_last_timestamp(last_timestamp)
                logger.info(f"write {row_count} rows")
            logger.info(f"end at: {last_timestamp}")
        except KeyboardInterrupt:
            self.set_last_timestamp(last_timestamp)
            logger.info("manual exit.")
        except NotFound:
            logger.info("collection not found.")

    def get_last_timestamp(self):
        file_name = f"{BASE_DIR}/temp/{self._day}_{self.name}"
        if os.path.exists(file_name):
            a = int(open(file_name, "r").read())
            b = self._init_timestamp
            return max(a, b)
        else:
            if not os.path.exists(f"{BASE_DIR}/temp/"):
                raise Exception("temp dir not exists")
            open(file_name, "w").write(f"{self._init_timestamp}")
            return self._init_timestamp

    def set_last_timestamp(self, last_timestamp):
        file_name = f"{BASE_DIR}/temp/{self._day}_{self.name}"
        if os.path.exists(file_name):
            return open(file_name, "w").write(str(last_timestamp))
        else:
            if not os.path.exists(f"{BASE_DIR}/temp/"):
                raise Exception("temp dir not exists")
            open(file_name, "w").write(f"{self._init_timestamp}")
            return 0
