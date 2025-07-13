import os
from airflow.models import Variable
from datetime import datetime, timedelta

ENV = os.environ['ENV']
IS_PRODUCTION = ENV == 'production'

PF_PARTNER_API_EVENTS_TOKEN = Variable.get('PF_PARTNER_API_EVENTS_TOKEN')
PF_PARTNER_API_TRANSACTIONS_TOKEN = Variable.get('PF_PARTNER_API_TRANSACTIONS_TOKEN')
PF_PARTNER_API_URL=Variable.get('PF_PARTNER_API_URL')
PF_PARTNER_APP_ID=Variable.get('PF_PARTNER_APP_ID')

ONETICK_PARTNER_API_URL = Variable.get('ONETICK_PARTNER_API_URL')
ONETICK_PARTNER_API_EVENTS_TOKEN = Variable.get('ONETICK_PARTNER_API_EVENTS_TOKEN')
ONETICK_PARTNER_APP_ID=Variable.get('ONETICK_PARTNER_APP_ID')

VIBE_PARTNER_API_URL = Variable.get('VIBE_PARTNER_API_URL')
VIBE_PARTNER_API_EVENTS_TOKEN = Variable.get('VIBE_PARTNER_API_EVENTS_TOKEN')
VIBE_PARTNER_APP_ID = Variable.get('VIBE_PARTNER_APP_ID')

GCP_PROJECT = Variable.get('GCP_PROJECT')
GCS_BUCKET = Variable.get('GCS_BUCKET')

START_DATE = datetime.now() - timedelta(days=1)

GLOBAL_WAIT_TIME = 1.5

API_CONFIGS = [
    {
        'url': ONETICK_PARTNER_API_URL,
        'access_token': ONETICK_PARTNER_API_EVENTS_TOKEN,
        'api_type': 'events',
        'id': ONETICK_PARTNER_APP_ID,
        'prefix': 'onetick',
        'type': 'apps'
    },
    {        
        'url': PF_PARTNER_API_URL,
        'access_token': PF_PARTNER_API_TRANSACTIONS_TOKEN,
        'api_type': 'transactions',
        'id': PF_PARTNER_APP_ID,
        'prefix': 'pf',
        'type': 'apps'
    },
    {
        'url': PF_PARTNER_API_URL,
        'access_token': PF_PARTNER_API_EVENTS_TOKEN,
        'api_type': 'events',
        'id': PF_PARTNER_APP_ID,
        'prefix': 'pf',
        'type': 'apps'
    },
    {
        'url': VIBE_PARTNER_API_URL,
        'access_token': VIBE_PARTNER_API_EVENTS_TOKEN,
        'api_type': 'events',
        'id': VIBE_PARTNER_APP_ID,
        'prefix': 'vibe',
        'type': 'apps'
    }
]

