import time
import pandas as pd
from operator import attrgetter
from dags.partner_api import IS_PRODUCTION, GLOBAL_WAIT_TIME
import logging
from datetime import timedelta, datetime

logger = logging.getLogger(__name__)

class APIResponseError(Exception):
    pass
class DataFetcher:
    def __init__(self,api_client,api_type,query_builder,latest_fetch_date, latest_fetch_data):
        self.api_client = api_client
        self.api_type=api_type
        self.query_builder=query_builder
        self.latest_fetch_date = latest_fetch_date
        self.latest_fetch_data = latest_fetch_data
    #[START fetch_data]
    def fetch_data(self):
        """ 
            This function will fetch data based on the last fetched date. If the last fetched date is found, the function will start fetching from 1 day before that last fetched date until current date.
            All the data of these fetched date will replace the current files data in GCS.
            This will help easier for debugging or re-run the script anytime without losing any data or relying on last fetched cursor text file.
        """
        timeMin=''
        if not self.latest_fetch_data.empty and self.latest_fetch_date:
            prev_date = datetime.strptime(self.latest_fetch_date, '%Y-%m-%d') - timedelta(days=1)
            formated_min_date = prev_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            if self.api_type == 'events':
                timeMin = f',occurredAtMin:"{formated_min_date}"'
            if self.api_type == 'transactions':
                timeMin = f', createdAtMin:"{formated_min_date}"'

        has_more_data=True
        wait_time=GLOBAL_WAIT_TIME
        rows_to_insert=[]
        cursor=None
        while has_more_data:
            pagination = f', after:"{cursor}"' if cursor is not None else ''
            query=query=self.query_builder.get_query(pagination=pagination, timeMin=timeMin)
            print(f"The query will be {query}")
            if cursor:
                print(f'Fetching data at cursor {cursor}')
            
            data=self.api_client.execute_query(query=query)
            # print(data)
            edges = self._get_edges(data)
            for edge in edges:
                row = self.transform_edge(edge)
                rows_to_insert.append(row)
            if edges:
                cursor=edges[-1]['cursor']
            else:
                has_more_data=False
            # if not IS_PRODUCTION:
            #     break
            time.sleep(wait_time)
        return pd.DataFrame(rows_to_insert)
    #[END fetch_data]

    #[START _get_edges_]
    def _get_edges(self, data):
        """
            Within API response, there will be edges object that contains major data of events or transactions. 
            This function will help to get the edges to make it easier for transformation
        """
        try:
            if self.api_type == 'transactions':
                return data['data']['transactions']['edges']
            elif self.api_type == 'events':
                return data['data']['app']['events']['edges']
            else:
                raise ValueError(f"Unknown API type: {self.api_type}")
        except KeyError as e:
            raise APIResponseError(f"Unexpected response structure. Missing key: {str(e)}. Response: {data}")
    #[END _get_edges]
    #[START transform_edges]
    def transform_edge(self,edges):
        self.edges=edges
        node = edges['node']
        cursor = edges['cursor']
        #[START safe_get]
        def safe_get(obj, attr, default=None):
            """
            Safely get an attribute or key from an object, even if it's None.
            Works with both objects (using getattr) and dictionaries (using get).
            Returns the default value if the attribute/key's value is None.
            """
            if obj is None:
                return default
            
            if isinstance(obj, dict):
                value = obj.get(attr)
                return default if value is None else value
            try:
                value = attrgetter(attr)(obj)
                return default if value is None else value
            except AttributeError:
                return default
        #[END safe_get]
        #[START sanitize_field]
        def sanitize_field(value):
            if isinstance(value, str):
                return value.replace('\n', ' ').replace('\r', ' ')
            return value
        #[END sanitize_field]

        if self.api_type == 'transactions':
            return {
                "createdAt": node["createdAt"],
                "id": node["id"],
                "chargeId": node.get("chargeId", "null"),
                "grossAmount": float(safe_get(node, "grossAmount", {}).get("amount", 0.0)),
                "currencyCode": safe_get(node,"grossAmount", {}).get("currencyCode", ''),
                "netAmount": float(node.get("netAmount", {}).get("amount", 0.0)),
                "shopID": node.get("shop", {}).get("id", ''),
                "shopDomain": node.get("shop", {}).get("myshopifyDomain",''),
                "shopifyFee": float(safe_get(node, "shopifyFee", {}).get("amount", 0.0)),
                "cursor": cursor
            }
        if self.api_type == 'events':
            return {
                "eventType" : sanitize_field(node['type']),
                "occurredAt" : sanitize_field(node['occurredAt']),
                "shopDomain" : sanitize_field(node.get('shop', {}).get('myshopifyDomain', '')),
                "shopName" : sanitize_field(node.get('shop', {}).get('name', '')),
                "shopId" : node.get('shop', {}).get('id', ''),
                "appName" : sanitize_field(node.get('app', {}).get('name', '')),
                "reason" : sanitize_field(node.get('reason', '')),
                "chargeAmount" : float(node.get('charge', {}).get('amount', {}).get('amount', 0.0)),
                "currencyCode" : node.get('charge', {}).get('amount', {}).get('currencyCode', ''),
                "billingOn" : node.get('charge', {}).get('billingOn', ''),
                "chargeId" : node.get('charge', {}).get('id', ''),
                "chargeName" : sanitize_field(node.get('charge', {}).get('name', '')),
                "cursor": cursor
            }
    