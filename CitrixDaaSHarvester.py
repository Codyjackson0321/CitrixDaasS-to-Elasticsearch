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
        self.access_token_citrix = f"CwsAuth bearer={r.json()['access_token']}",
        self.access_token_odata = f"CWSAuth Bearer={r.json()['access_token']}",

    def get_logs_citrix_cloud(self):
        self.headers['Authorization'] = self.access_token_citrix[0]
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

        processed_logs = self.process_logs(response.json()['items'], "citrix_cloud")
        self.write_logs(processed_logs)

    def get_logs_citrix_daas(self):
        self.headers = {
            "accept": "application/json",
            "Content-Type": "application/json",
        }
        self.headers['Authorization'] = self.access_token_citrix[0]
        self.headers['Citrix-CustomerId'] = self.config['CUSTOMER_ID']
        self.headers['Citrix-InstanceId'] = self.config['SITE_ID']

        # Use GET endpoint with 'days' parameter for date filtering
        # The API doesn't support date filtering via SearchFilters in POST $search
        response = requests.get(
            self.api_url + "cvad/manage/ConfigLog/Operations",
            headers=self.headers,
            params={
                "days": 1,  # Change to 1 for 1 day, or adjust as needed
                "limit": 1000  # Maximum number of results to return
            }
        )


        processed_logs = self.process_logs(response.json()['Items'], "citrix_daas")
        self.write_logs(processed_logs)


    def get_logs_user_sessions(self):
        self.headers = {
            "accept": "application/json",
            "Content-Type": "application/json",
        }
        self.headers['Authorization'] = self.access_token_odata[0]
        self.headers['Citrix-CustomerId'] = self.config['CUSTOMER_ID']

        start = self.current_time - timedelta(minutes=11)
        start_str = start.strftime("%Y-%m-%dT%H:%M:%SZ")

        # Get sessions that started or ended in the last 11 minutes
        response = requests.get(
            self.api_url + "monitorodata/Sessions",
            headers=self.headers,
            params={
                "$filter": f"StartDate ge {start_str} or EndDate ge {start_str}",
                "$expand": "User,Machine"
            }
        )

        if response.status_code == 200:
            processed_logs = self.process_logs(response.json()['value'], "user_sessions")
            self.write_logs(processed_logs)
        else:
            print(f"Error fetching user sessions: {response.status_code}")
            print(response.text)

    def process_logs(self, logs, category):
        parsed_logs = []
        for log in logs:
            doc = {
                "_op_type": "create",
                "_index": self.data_stream_name,
                "_source": {
                    "event": {
                        "original": json.dumps(log),
                        "category": category
                    },
                    "citrix": log
                }
            }

            if category == "citrix_cloud":
                if 'recordId' in log:
                    doc['_id'] = log['recordId']

                if 'eventType' in log:
                    doc['_source']['event']['action'] = log['eventType']

                if "actorDisplayName" in log:
                    doc['_source']['user'] = {
                        "name": log['actorDisplayName'],
                        "related": [log['actorDisplayName']]
                    }
                if "message" in log:
                    doc['_source']['message'] = log['message']['en-US']
                if "utcTimestamp" in log:
                    doc['_source']['@timestamp'] = log['utcTimestamp']

            elif category == "citrix_daas":
                print(log)
                if 'Id' in log:
                    doc['_id'] = log['Id']
                if "User" in log:
                    doc['_source']['user'] = {
                        "name": log['User'],
                        "related": [log['User']]
                    }
                if "OperationType" in log:
                    doc['_source']['event']['action'] = log['OperationType']

                if "StartTime" in log:
                    doc['_source']['@timestamp'] = log['StartTime']

                if "Text" in log:
                    doc['_source']['message'] = log['Text']

            elif category == "user_sessions":
                if 'SessionKey' in log:
                    doc['_id'] = log['SessionKey']
                
                # Determine event action based on session state
                if log.get('EndDate'):
                    doc['_source']['event']['action'] = 'user-logoff'
                    doc['_source']['@timestamp'] = log['EndDate']
                    doc['_source']['message'] = f"User session ended"
                elif log.get('StartDate'):
                    doc['_source']['event']['action'] = 'user-login'
                    doc['_source']['@timestamp'] = log['StartDate']
                    doc['_source']['message'] = f"User session started"
                
                # User information
                if 'User' in log and log['User']:
                    user_name = log['User'].get('UserName') or log['User'].get('Upn')
                    if user_name:
                        doc['_source']['user'] = {
                            "name": user_name,
                            "related": [user_name]
                        }
                
                # Machine/host information
                if 'Machine' in log and log['Machine']:
                    if 'DnsName' in log['Machine']:
                        doc['_source']['host'] = {
                            "name": log['Machine']['DnsName']
                        }
                
                # Session details
                if 'SessionType' in log:
                    session_types = {0: 'Application', 1: 'Desktop'}
                    doc['_source']['citrix']['session_type'] = session_types.get(log['SessionType'], 'Unknown')
                
                if 'ConnectionState' in log:
                    connection_states = {0: 'Unknown', 1: 'Connected', 2: 'Disconnected', 3: 'Terminated', 4: 'PreparingSession', 5: 'Active', 6: 'Reconnecting', 7: 'NonBrokeredSession', 8: 'Other', 9: 'Pending'}
                    doc['_source']['citrix']['connection_state'] = connection_states.get(log['ConnectionState'], 'Unknown')

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
    citrix.get_logs_citrix_cloud()
    citrix.get_logs_citrix_daas()
    citrix.get_logs_user_sessions()

if __name__ == "__main__":
    main()