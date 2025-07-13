from airflow.decorators  import dag, task
from dags.partner_api.make_request import MakeRequest
from dags.partner_api.query_builder import QueryBuilder
from dags.partner_api.data_fetcher import DataFetcher
from dags.partner_api.data_loader import DataLoader
from dags.partner_api.check_history import GCSDataChecker
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from plugins.notifications.main import SlackNotification
from dags.partner_api import API_CONFIGS,GCP_PROJECT, GCS_BUCKET, START_DATE

slack_notifier = SlackNotification()

DEAFAULT_DAG_ARGS = {
    'owner': 'dongnv',
    'retries':0,
    'on_failure_callback': slack_notifier.notify_failure
}
SCHEDULE_INTERVAL = '30 0 * * *'
DAG_VERSION = '1.0.0'

#Starting looping through every configs to create DAGs based on it
for config in API_CONFIGS:
    @dag (
        dag_id=f"partner_api_{config['prefix']}_{config['api_type']}",
        default_args=DEAFAULT_DAG_ARGS,
        schedule_interval=SCHEDULE_INTERVAL,
        start_date=START_DATE,
        catchup=False,
        tags=['partner_api', config['prefix'], config['type'], config['api_type']]
    )
    #[START dynamic_api_pipeline]
    def dynamic_api_pipeline():
        """
        Define and instantiate relevant variable to helps all worker objects to run.
        """
        dag_id = f"partner_api_{config['prefix']}_{config['api_type']}"
        api_type= config['api_type']
        id = config['id']
        url = config['url']
        access_token = config['access_token']
        folder_type = config['type']
        prefix = config['prefix']
        history_check = GCSDataChecker(bucket_name=GCS_BUCKET,folder_path_prefix=f'partner_api/{folder_type}/{prefix}/{api_type}',gcs_conn_id='google_cloud_default')
        query_builder = QueryBuilder(api_type=api_type, id=id)
        api_client = MakeRequest(url, access_token, api_type, query=None)
        root_folder = f'partner_api/{folder_type}/{prefix}/{api_type}'


        @task
        def find_last_fetch_date():
            last_fetch_date = history_check.get_latest_dt_folder()
            return last_fetch_date
        @task
        def find_last_fetch_data():
            last_fetch_folder = history_check.fetch_files_from_latest_df()
            return last_fetch_folder

        @task(pool='api_request_limit')
        def fetch_data(api_client, api_type, query_builder,latest_fetch_date, latest_fetch_data):
            fetcher = DataFetcher(api_client=api_client, api_type=api_type,
                                  query_builder=query_builder,latest_fetch_date=latest_fetch_date,latest_fetch_data=latest_fetch_data)
            data = fetcher.fetch_data()
            return data
        
        # No longer need this tasks since the mechanism will replace files with latest data
        #  @task
        # def remove_matching_rows(df1, df2, column='cursor'):
        #     if df1.empty:
        #         return df2
        #     else:
        #         matching_values = df1[column].unique()
        #         filtered_df2 = df2[~df2[column].isin(matching_values)]
        #         return filtered_df2

        @task
        def load_data(fetch_result,root_folder,api_type,bucket_name):
            loader = DataLoader(df=fetch_result,root_folder=root_folder,api_type=api_type,bucket_name=bucket_name)
            loader.load_to_gcs()

        def create_bronze_table(dag_id, root_folder, project_id, dataset_id, bucket_name):
            """
            Create an external partitioned table from the defined GCS path using Hive partitioning.
            """
            table_name = f'{dag_id}_bronze'
            gcs_path = f'gs://{bucket_name}/{root_folder}/*'
            hive_partition_uri_prefix = f'gs://{bucket_name}/{root_folder}/'

            return BigQueryInsertJobOperator(
                task_id='create_bronze_table',
                configuration={
                    "query": {
                        "query": f"""
                            CREATE EXTERNAL TABLE IF NOT EXISTS `{project_id}.{dataset_id}.{table_name}`
                            WITH PARTITION COLUMNS
                            OPTIONS (
                                format = 'PARQUET',
                                uris = ['{gcs_path}'],
                                hive_partition_uri_prefix = '{hive_partition_uri_prefix}'
                            )
                        """,
                        "useLegacySql": False  # Ensures that standard SQL is used
                    }
                },
                gcp_conn_id='google_cloud_default'
            )
        
        @task(trigger_rule='all_success')
        def notify_success_dag(**kwargs):
            slack_notifier.notify_success(kwargs)
       
        latest_fetch_date = find_last_fetch_date()
        latest_fetch_data = find_last_fetch_data()
        fetching_data = fetch_data(api_client,api_type,query_builder,latest_fetch_date,latest_fetch_data) 
        # fetch_result = remove_matching_rows(df1=latest_fetch_data,df2=fetching_data,column='cursor')
        load_task = load_data(fetching_data, root_folder, api_type, bucket_name=GCS_BUCKET)
        create_bronze_table_task = create_bronze_table(
            dag_id=dag_id,
            root_folder=root_folder,
            project_id=GCP_PROJECT,
            dataset_id='partner_api',
            bucket_name=GCS_BUCKET
        )
        notify_success = notify_success_dag()

        #TASKS dependancies
        latest_fetch_date >> latest_fetch_data >> fetching_data >> load_task >> create_bronze_table_task >> notify_success
        #[END dynamic_api_pipeline]
    
    globals()[f"partner_api_{config['prefix']}_{config['api_type']}"] = dynamic_api_pipeline()


        


                