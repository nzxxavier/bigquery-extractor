from . import Driver
from google.cloud import bigquery


class BigqueryDriver(Driver):
    def __init__(self):
        self.client = bigquery.Client()
        self.query_job = None

    def insert(self, query, data):
        pass

    def update(self, data: dict):
        pass

    def delete(self, data: dict):
        pass

    def query(self, query):
        self.query_job = self.client.query(query)

    def get_result(self, offset=0):
        return self.query_job.result(page_size=1000, start_index=offset).to_arrow_iterable()
