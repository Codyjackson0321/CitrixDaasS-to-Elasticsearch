from elasticsearch import Elasticsearch
from elasticsearch.helpers import parallel_bulk
from dotenv import dotenv_values
from datetime import datetime, timedelta, UTC
import requests
import json
import argparse

class CitrixDaasToElasticsearch:
    def __init__(self, sync_mode=False):
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
        self.sync_mode = sync_mode

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
        headers = {
            "accept": "application/json",
            "Authorization": self.access_token_citrix[0],
            "Citrix-CustomerId": self.config['CUSTOMER_ID']
        }

        # Use maximum range for sync mode, otherwise 11 minutes for polling
        if self.sync_mode:
            start = self.current_time - timedelta(days=3650)  # ~10 years max
        else:
            start = self.current_time - timedelta(minutes=11)
        start_str = start.strftime("%Y-%m-%dT%H:%M:%S")

        response = requests.get(
            self.api_url + "systemlog/records",
            headers=headers,
            params={
                "StartDateTime": start_str,
            }
        )

        processed_logs = self.process_logs(response.json()['items'], "citrix_cloud")
        self.write_logs(processed_logs)

    def get_logs_citrix_daas(self):
        headers = {
            "accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": self.access_token_citrix[0],
            "Citrix-CustomerId": self.config['CUSTOMER_ID'],
            "Citrix-InstanceId": self.config['SITE_ID']
        }

        # Use GET endpoint with 'days' parameter for date filtering
        # The API doesn't support date filtering via SearchFilters in POST $search
        response = requests.get(
            self.api_url + "cvad/manage/ConfigLog/Operations",
            headers=headers,
            params={
                "days": 365 if self.sync_mode else 40,
                "limit": 1000  # Maximum number of results to return
            }
        )

        if response.status_code == 200:
            data = response.json()
            if 'Items' in data and len(data['Items']) > 0:
                processed_logs = self.process_logs(data['Items'], "citrix_daas")
                self.write_logs(processed_logs)
                print(f"Processed {len(data['Items'])} DaaS config logs")
            else:
                print(f"No DaaS config logs found in response. Response keys: {data.keys() if data else 'empty'}")
        else:
            print(f"Error fetching DaaS config logs: {response.status_code}")
            print(f"Response: {response.text}")

    def get_logs_user_sessions(self):
        headers = {
            "accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": self.access_token_odata[0],
            "Citrix-CustomerId": self.config['CUSTOMER_ID']
        }

        # Use maximum range for sync mode, otherwise 11 minutes for polling
        if self.sync_mode:
            start = self.current_time - timedelta(days=365)
        else:
            start = self.current_time - timedelta(minutes=11)
        start_str = start.strftime("%Y-%m-%dT%H:%M:%SZ")

        # Get sessions that started or ended in the time range
        response = requests.get(
            self.api_url + "monitorodata/Sessions",
            headers=headers,
            params={
                "$filter": f"StartDate ge {start_str} or EndDate ge {start_str}",
                "$expand": "User,Machine"
            }
        )

        if response.status_code == 200:
            data = response.json()
            sessions = data.get('value', [])
            print(f"Fetched {len(sessions)} user sessions from API")
            
            # Check if there's a continuation token for pagination
            if '@odata.nextLink' in data:
                print(f"WARNING: More sessions available (pagination detected). Only processing first batch.")
            
            if sessions:
                processed_logs = self.process_logs(sessions, "user_sessions")
                self.write_logs(processed_logs)
            else:
                print("No user sessions found in the specified time range")
        else:
            print(f"Error fetching user sessions: {response.status_code}")
            print(response.text)

    def process_logs(self, logs, category):
        parsed_logs = []
        for log in logs:
            # Make a copy of the log to avoid modifying the original
            log_copy = log.copy()
            
            # For user_sessions, rewrite the User object to just the username string
            if category == "user_sessions" and 'User' in log_copy and isinstance(log_copy['User'], dict):
                user_name = log_copy['User'].get('UserName') or log_copy['User'].get('Upn') or 'unknown'
                log_copy['User'] = user_name
            
            doc = {
                "_op_type": "create",
                "_index": self.data_stream_name,
                "_source": {
                    "event": {
                        "original": json.dumps(log),
                        "category": category
                    },
                    "citrix": log_copy
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
                    # Convert from "MM/DD/YYYY h:mm:ss AM/PM" to ISO 8601
                    try:
                        from datetime import datetime
                        dt = datetime.strptime(log['StartTime'], "%m/%d/%Y %I:%M:%S %p")
                        doc['_source']['@timestamp'] = dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                    except Exception as e:
                        print(f"Error parsing StartTime '{log['StartTime']}': {e}")
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

                # User information - check multiple possible fields
                user_name = None
                if 'User' in log and log['User']:
                    user_name = log['User'].get('UserName') or log['User'].get('Upn')
                elif 'AssociatedUserFullNames' in log and log['AssociatedUserFullNames']:
                    user_name = log['AssociatedUserFullNames'][0] if isinstance(log['AssociatedUserFullNames'], list) else log['AssociatedUserFullNames']
                
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

            parsed_logs.append(doc)
        return parsed_logs

    def write_logs(self, logs):
        if not logs:
            print("No logs to write")
            return
        
        success_count = 0
        duplicate_count = 0
        error_count = 0
        
        try:
            for success, info in parallel_bulk(self.es, actions=logs, raise_on_error=False):
                if success:
                    success_count += 1
                else:
                    # Check if it's a duplicate (409 conflict)
                    if info.get('create', {}).get('status') == 409:
                        duplicate_count += 1
                    else:
                        error_count += 1
                        print(f"Failed to index document: {info}")
            
            print(f"Indexing complete - Success: {success_count}, Duplicates: {duplicate_count}, Errors: {error_count}")
        except Exception as e:
            print(f"Error during bulk indexing: {e}")
            if hasattr(e, 'errors'):
                for error in e.errors:
                    print(f"Error detail: {error}")

def main():
    parser = argparse.ArgumentParser(description='Harvest Citrix DaaS logs to Elasticsearch')
    parser.add_argument('--sync', action='store_true', 
                        help='Enable sync mode to fetch maximum historical data (365 days for DaaS, ~10 years for Cloud)')
    args = parser.parse_args()

    citrix = CitrixDaasToElasticsearch(sync_mode=args.sync)
    citrix.get_token()
    citrix.get_logs_citrix_cloud()
    citrix.get_logs_citrix_daas()
    citrix.get_logs_user_sessions()

if __name__ == "__main__":
    main()