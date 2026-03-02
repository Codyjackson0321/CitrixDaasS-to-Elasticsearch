from elasticsearch import Elasticsearch
from elasticsearch.helpers import parallel_bulk
from dotenv import dotenv_values
from datetime import datetime, timedelta, UTC
import requests
import json

class CitrixDaasToElasticsearch:
    def __init__(self):
        self.config = dotenv_values(".env")
        self.api_url = f"https://api.cloud.com/"
        self.headers = {
            "accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        self.es = Elasticsearch(
            hosts=self.config['ELASTIC_URL'],
            api_key=self.config['ELASTIC_API_KEY']
        )
        self.data_stream_name = "logs-citrix.daas-default"
        self.access_token = ""
        self.current_time = datetime.now(UTC)

    def get_token(self):
        r = requests.post(
            self.api_url + f"cctrustoauth2/{self.config['CUSTOMER_ID']}/tokens/clients",
            headers=self.headers,
            data={
                "grant_type": "client_credentials",
                "client_id": self.config['CITRIX_CLIENT_ID'],
                "client_secret": self.config['CITRIX_CLIENT_SECRET']
            }
        )
        self.access_token = f"CwsAuth Bearer={r.json()['access_token']}",

    def get_logs(self):
        if self.access_token:
            self.headers['Authorization'] = self.access_token[0]
            self.headers['Citrix-CustomerId'] = self.config['CUSTOMER_ID']

        start = self.current_time - timedelta(minutes=11)
        start_str = start.strftime("%Y-%m-%dT%H:%M:%S")

        response = requests.get(
            self.api_url + "systemlog/records",
            headers=self.headers,
            params={
                "StartDateTime": start_str,
            }
        )
        processed_logs = self.process_logs(response.json()['items'])
        self.write_logs(processed_logs)

    def process_logs(self, logs):
        parsed_logs = []
        for log in logs:
            doc = {
                "_op_type": "create",
                "_index": self.data_stream_name,
                "_id": log['recordId'],
                "_source": {
                    "@timestamp": log['utcTimestamp'],
                    "event": {
                        "original": json.dumps(log)
                    },
                    "citrix": log,
                    "message": log['message']['en-US']
                }
            }
            if "actorDisplayName" in log:
                doc['_source']['user'] = {
                    "name": log['actorDisplayName']
                }

            parsed_logs.append(doc)
        return parsed_logs

    def write_logs(self, logs):
        try:
            for success, info in parallel_bulk(self.es, actions=logs, raise_on_error=False):
                # Ignore 409s
                if not success and not info['create']['status'] == 409:
                    print(f"Failed to index document: {info}")
        except Exception as e:
            print(f"Error during bulk indexing: {e}")
            if hasattr(e, 'errors'):
                for error in e.errors:
                    print(f"Error detail: {error}")

def main():
    citrix = CitrixDaasToElasticsearch()
    citrix.get_token()
    citrix.get_logs()

if __name__ == "__main__":
    main()