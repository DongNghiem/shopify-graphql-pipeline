import requests
import logging

logger = logging.getLogger(__name__)

class MakeRequest:
    """
        This class helps initalize params for the API request. 
        Those params are crucial to help execute query and get data from the API
    """
    def __init__(self,url,access_token,api_type,query=None):
        self.url = url
        self.headers = {"X-Shopify-Access-Token": f"{access_token}",
                        "Content-Type": "application/json"}
        self.api_type = api_type
        self.query=query
    #[START execute_query]
    def execute_query(self, query):
        try:
            response = requests.post(self.url, headers=self.headers, json={'query':query})
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"API request failed: {str(e)}")
            logger.error(f"Response content: {e.response.content if e.response else 'No response'}")
            raise
    #[END execute_query]
