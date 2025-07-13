import sys
import json
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
import logging
from datetime import datetime
import urllib.request
import time
import pandas as pd
import pyspark.sql.functions as F

# Initialize logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)  # Set to INFO for more detailed logs

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'DATA_SOURCE', 'DATABASE', 'COLLECTION'])

# Configuration
class Config:
    # AWS Configuration
    AWS_ACCOUNT = '656093429694'
    
    # S3 Configuration
    RAW_BUCKET = "ss-big-data-datalake-raw-production"
    CURATED_BUCKET = "ss-big-data-datalake-curated-production"
    
    # Glue Configuration
    GLUE_SERVICE_ROLE = f'arn:aws:iam::{AWS_ACCOUNT}:role/SS-Big-Data-Role-GlueWithLakeFormation-Production'
    CURATED_DATABASE = "ss-big-data-partnerapi-curated-production"
    
    # Slack Configuration
    SLACK_BOT_TOKEN = 'REMOVED_TOKEN294405122499-5505542413671-bu2sVXJ7RoHdzP6sOynC3Ley'
    SLACK_CHANNEL_ID = 'C07GVUDHABB'
    SLACK_ICON_URL = 'https://cdn.pagefly.io/images/custom/AwsGlueStreamlineSvgLogos11741748200367.png'
    
    # Data Configuration
    DATETIME_COLUMNS = [
        'createdat', 'deletedat', 'publishedat', 'updatedat', 
        'lastaccess', 'reinstalledat', 'metadata_created_at', 
        'uninstalledat', 'metadata_updated_at', 'occurredat'
    ]
    
    # Crawler Configuration
    CRAWLER_TIMEOUT = 1200  # 20 minutes timeout for crawlers

class Notification:
    def __init__(self, config):
        self.config = config
    
    def send_slack_alert(self, message):
        payload = {
            'channel': self.config.SLACK_CHANNEL_ID,
            'text': message,
            'username': args['JOB_NAME'],
            'icon_url': self.config.SLACK_ICON_URL
        }
        
        data = json.dumps(payload).encode('utf-8')
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f"Bearer {self.config.SLACK_BOT_TOKEN}"
        }
        
        request = urllib.request.Request(url="https://slack.com/api/chat.postMessage", data=data, headers=headers)
        urllib.request.urlopen(request)

# Other global variables
config = Config()
notifier = Notification(config)

class DataProcessor:
    """Handles data transformation operations"""
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config
        # Store both original and lowercase versions of datetime columns
        self.datetime_columns = {
            'occurredAt': 'occurredat',
            'createdAt': 'createdat'
        }

    def convert_datetime_columns(self, df):
        """Convert datetime columns to timestamp type using Spark"""
        for original_col, lower_col in self.datetime_columns.items():
            # Check for original column name
            if original_col in df.columns:
                logger.info(f"Converting column {original_col} to timestamp")
                df = df.withColumn(original_col, F.to_timestamp(F.col(original_col)))
            # Check for lowercase column name
            elif lower_col in df.columns:
                logger.info(f"Converting column {lower_col} to timestamp")
                df = df.withColumn(lower_col, F.to_timestamp(F.col(lower_col)))
        return df

class S3Handler:
    """Handles S3 read and write operations"""
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config
        self.s3_client = boto3.client('s3')

    def list_partitions(self, data_source, database, collection):
        """List all available partitions in the raw bucket"""
        prefix = f"{data_source}/{database}-{collection}/"
        
        logger.info(f"Listing partitions in s3://{self.config.RAW_BUCKET}/{prefix}")
        
        paginator = self.s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=self.config.RAW_BUCKET, Prefix=prefix, Delimiter='/')
        
        partitions = []
        for page in pages:
            if 'CommonPrefixes' in page:
                for obj in page['CommonPrefixes']:
                    partition_path = obj['Prefix']
                    if 'dt=' in partition_path:
                        partition_value = partition_path.split('dt=')[1].rstrip('/')
                        partitions.append(partition_value)
        
        if not partitions:
            logger.warning(f"No partitions found in s3://{self.config.RAW_BUCKET}/{prefix}")
        else:
            logger.info(f"Found {len(partitions)} partitions: {partitions}")
        
        return partitions

    def read_partition(self, raw_path):
        """Read data from raw bucket partition using Spark
        
        Args:
            raw_path (str): Path to the parquet file
        """
        logger.info(f"Reading data from {raw_path}")
        return self.spark.read.parquet(raw_path)

    def save_to_curated(self, df, curated_path):
        """Save data to curated bucket with explicit timestamp type
        
        Note: Explicitly specifying timestamp type ensures consistent schema inference
        by the crawler. This helps maintain the correct data type for datetime columns.
        """
        logger.info(f"Saving data to {curated_path}")
        
        # Check if occurredat/occurredAt exists and verify timestamp type
        datetime_col = None
        if 'occurredat' in df.columns:
            datetime_col = 'occurredat'
        elif 'occurredAt' in df.columns:
            datetime_col = 'occurredAt'
            
        if datetime_col:
            # Verify if column is timestamp type
            col_type = df.schema[datetime_col].dataType.typeName()
            if col_type != 'timestamp':
                error_msg = f"Column '{datetime_col}' is not timestamp type. Current type: {col_type}"
                logger.error(error_msg)
                raise ValueError(error_msg)
            logger.info(f"Verified {datetime_col} is timestamp type")
        
        # Write using Spark DataFrame
        df.write.mode("overwrite").parquet(curated_path)

class GlueHandler:
    """Handles Glue operations"""
    def __init__(self, catalog, service_role):
        self.glue = boto3.client('glue')
        self.catalog = catalog
        self.service_role = service_role
    
    def get_crawler_status(self, crawler_name):
        """Get the current status of a crawler"""
        try:
            response = self.glue.get_crawler(Name=crawler_name)
            return response['Crawler']['State']
        except self.glue.exceptions.EntityNotFoundException:
            return "NOT_FOUND"

    def wait_for_crawler(self, crawler_name, timeout=300):
        """Wait for crawler to finish running"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            status = self.get_crawler_status(crawler_name)
            if status == "READY":
                return True
            elif status in ["RUNNING", "STOPPING"]:
                time.sleep(10)  # Wait 10 seconds before checking again
                logger.info(f"Waiting for crawler {crawler_name}... Status: {status}")
            else:
                return False
        return False

    def create_database_if_not_exists(self, database_name, description=None):
        """Create a database in Glue Data Catalog if it doesn't exist"""
        try:
            self.glue.get_database(Name=database_name)
            logger.info(f"Database {database_name} already exists")
            return True
        except self.glue.exceptions.EntityNotFoundException:
            # Database doesn't exist, create it
            database_input = {
                'Name': database_name,
                'Description': description or f'Database {database_name} created automatically by Glue job'
            }
            
            self.glue.create_database(DatabaseInput=database_input)
            logger.info(f"Database {database_name} created successfully")
            return True

    def create_or_update_crawler(self, crawler_name, database_name, s3_path):
        """Configure crawler"""
        crawler_config = {
            "Name": crawler_name,
            "Role": self.service_role,
            "DatabaseName": database_name,
            "Targets": {
                "S3Targets": [
                    {"Path": s3_path}
                ]
            },
            "SchemaChangePolicy": {
                "UpdateBehavior": "UPDATE_IN_DATABASE",
                "DeleteBehavior": "LOG"
            },
            "RecrawlPolicy": {
                "RecrawlBehavior": "CRAWL_EVERYTHING"
            },
            "TablePrefix": "",
            "Configuration": json.dumps({
                "Version": 1.0,
                "CrawlerOutput": {
                    "Partitions": {
                        "AddOrUpdateBehavior": "InheritFromTable"
                    },
                    "Tables": {
                        "AddOrUpdateBehavior": "MergeNewColumns"
                    }
                },
                "Grouping": {
                    "TableGroupingPolicy": "CombineCompatibleSchemas"
                },
                "CreatePartitionIndex": False
            })
        }

        # Check crawler status first
        crawler_status = self.get_crawler_status(crawler_name)

        if crawler_status == "NOT_FOUND":
            # Crawler doesn't exist, create it
            logger.info(f"Creating crawler {crawler_name}")
            self.glue.create_crawler(**crawler_config)
        elif crawler_status == "READY":
            # Crawler exists and is not running, update it
            logger.info(f"Updating crawler {crawler_name}")
            self.glue.update_crawler(**crawler_config)
        elif crawler_status in ["RUNNING", "STOPPING"]:
            # Crawler is already running, wait for it to finish or skip update
            logger.info(f"Crawler {crawler_name} is currently {crawler_status}. Waiting for it to complete...")
            if self.wait_for_crawler(crawler_name, timeout=Config.CRAWLER_TIMEOUT):
                logger.info(f"Crawler {crawler_name} is now ready. Updating...")
                self.glue.update_crawler(**crawler_config)
            else:
                logger.warning(f"Crawler {crawler_name} is still {crawler_status} after timeout. Skipping update.")
        else:
            # Other states like FAILED
            logger.info(f"Crawler {crawler_name} is in {crawler_status} state. Attempting to update...")
            self.glue.update_crawler(**crawler_config)

    def run_crawler(self, crawler_name, max_attempts=40, delay=30):
        """Wait for crawler to complete its run."""
        # Check the current state of the crawler
        crawler_status = self.get_crawler_status(crawler_name)
        
        if crawler_status == "RUNNING":
            logger.info(f"Crawler {crawler_name} is already running. Waiting for completion...")
        elif crawler_status == "STOPPING":
            logger.info(f"Crawler {crawler_name} is stopping. Waiting for it to complete before starting...")
            if not self.wait_for_crawler(crawler_name, timeout=Config.CRAWLER_TIMEOUT):
                logger.error(f"Crawler {crawler_name} did not stop after waiting period")
                return False
            logger.info(f"Starting crawler {crawler_name}")
            self.glue.start_crawler(Name=crawler_name)
        elif crawler_status == "READY":
            logger.info(f"Starting crawler {crawler_name}")
            self.glue.start_crawler(Name=crawler_name)
        else:
            logger.info(f"Crawler {crawler_name} is in state {crawler_status}. Attempting to start...")
            try:
                self.glue.start_crawler(Name=crawler_name)
            except Exception as e:
                logger.error(f"Failed to start crawler {crawler_name}: {str(e)}")
                return False
        
        # Wait for crawler to complete
        for attempt in range(max_attempts):
            try:
                response = self.glue.get_crawler(Name=crawler_name)
                state = response['Crawler']['State']
                
                if state == 'READY':
                    last_crawl = response['Crawler'].get('LastCrawl', {})
                    if last_crawl.get('Status') == 'SUCCEEDED':
                        logger.info(f"Crawler {crawler_name} completed successfully")
                        return True
                    elif last_crawl.get('Status') == 'FAILED':
                        logger.error(f"Crawler {crawler_name} failed: {last_crawl.get('ErrorMessage', 'Unknown error')}")
                        return False
                    else:
                        logger.info(f"Crawler {crawler_name} completed with status: {last_crawl.get('Status')}")
                        return True
                elif state == 'FAILED':
                    logger.error(f"Crawler {crawler_name} failed")
                    return False
                
                logger.info(f"Crawler {crawler_name} is {state}. Waiting... (attempt {attempt+1}/{max_attempts})")
                time.sleep(delay)
            except Exception as e:
                logger.error(f"Error checking crawler status: {str(e)}")
                time.sleep(delay)
        
        logger.error(f"Crawler {crawler_name} did not complete after {max_attempts} attempts")
        return False

    def get_table(self, database_name, name):
        """Get table definition"""
        return self.glue.get_table(
            DatabaseName=database_name,
            Name=name
        )

class LakeFormationManager:
    """Handles Lake Formation permissions and column tagging"""
    def __init__(self, config):
        self.config = config
        self.lf = boto3.client('lakeformation')
        self.catalog = config.AWS_ACCOUNT
        self.region = boto3.session.Session().region_name
        self.service_role = config.GLUE_SERVICE_ROLE

    def analyze_column_for_tags(self, column_name):
        """Determine LF-Tags based on column name"""
        tags = {}
        if column_name in self.config.DATETIME_COLUMNS:
            tags['Allow_duplicated'] = 'yes'
            tags['Allow_null'] = 'no'
            return tags
        if column_name == 'shopid':
            tags['Allow_null'] = 'no'
            tags['Allow_duplicated'] = 'no'
            return tags
        tags['Allow_duplicated'] = 'yes'
        tags['Allow_null'] = 'yes'
        return tags

    def grant_permissions(self, database_name, table_name=None):
        """Grant Lake Formation permissions"""
        try:
            self.lf.get_effective_permissions_for_path(
                ResourceArn=f'arn:aws:glue:{self.region}:{self.catalog}:database/{database_name}'
            )
        except self.lf.exceptions.EntityNotFoundException:
            self.lf.grant_permissions(
                Principal={'DataLakePrincipalIdentifier': self.service_role},
                Resource={'Database': {
                    'CatalogId': self.catalog,
                    'Name': database_name
                }},
                Permissions=['ALL'],
                PermissionsWithGrantOption=['ALL']
            )
            
        if table_name:
            self.lf.grant_permissions(
                Principal={'DataLakePrincipalIdentifier': self.service_role},
                Resource={'Table': {
                    'CatalogId': self.catalog,
                    'DatabaseName': database_name,
                    'Name': table_name
                }},
                Permissions=['ALL'],
                PermissionsWithGrantOption=['ALL']
            )
            
            self.lf.grant_permissions(
                Principal={'DataLakePrincipalIdentifier': self.service_role},
                Resource={'TableWithColumns': {
                    'CatalogId': self.catalog,
                    'DatabaseName': database_name,
                    'Name': table_name,
                    'ColumnWildcard': {}
                }},
                Permissions=['SELECT'],
                PermissionsWithGrantOption=['SELECT']
            )

    def process_and_apply_tags(self, glue_handler, database_name, table_name):
        """Process and apply tags to all columns"""
        table_def = glue_handler.get_table(
            database_name=database_name,
            name=table_name
        )

        for column in table_def['Table']['StorageDescriptor']['Columns']:
            tags = self.analyze_column_for_tags(column_name=column['Name'])
            
            for tag_key, tag_value in tags.items():
                self.apply_tag_to_column(
                    database_name=database_name,
                    table_name=table_name,
                    column_name=column['Name'],
                    tag_key=tag_key,
                    tag_value=tag_value
                )

    def apply_tag_to_column(self, database_name, table_name, column_name, tag_key, tag_value):
        """Apply a single tag to a column"""
        self.lf.add_lf_tags_to_resource(
            CatalogId=self.catalog,
            Resource={
                'TableWithColumns': {
                    'CatalogId': self.catalog,
                    'DatabaseName': database_name,
                    'Name': table_name,
                    'ColumnNames': [column_name]
                }
            },
            LFTags=[{
                'CatalogId': self.catalog,
                'TagKey': tag_key,
                'TagValues': [tag_value]
            }]
        )

class GlueJob:
    """Orchestrates the ETL workflow"""
    def __init__(self, glue_context, config):
        self.glue_context = glue_context
        self.job = Job(self.glue_context)
        self.spark = self.glue_context.spark_session
        self.config = config
        
        # Initialize workers
        self.s3_handler = S3Handler(self.spark, config)
        self.data_processor = DataProcessor(self.spark, config)
        self.glue_handler = GlueHandler(config.AWS_ACCOUNT, config.GLUE_SERVICE_ROLE)
        self.lf_manager = LakeFormationManager(config)
        
        # Initialize job-specific variables
        self.crawler_name = f"ss-big-data-{args['DATA_SOURCE']}-{args['DATABASE']}-{args['COLLECTION']}-curated"
        self.curated_table_name = f"{args['DATABASE']}_{args['COLLECTION']}_curated".lower()

    def execute(self):
        """Execute the ETL workflow for bulk processing"""
        try:
            self.job.init(args['JOB_NAME'], args)

            # 1. List all partitions
            partitions = self.s3_handler.list_partitions(
                data_source=args['DATA_SOURCE'],
                database=args['DATABASE'],
                collection=args['COLLECTION']
            )
            
            if not partitions:
                raise Exception(f"No partitions found for {args['DATA_SOURCE']}/{args['DATABASE']}-{args['COLLECTION']}")
            
            processed_count = 0
            all_paths = []
            
            # 2. Process each partition
            for partition_value in partitions:
                try:
                    logger.info(f"Processing partition: {partition_value}")
                    
                    # Define paths directly
                    raw_path = f"s3://{self.config.RAW_BUCKET}/{args['DATA_SOURCE']}/{args['DATABASE']}-{args['COLLECTION']}/dt={partition_value}"
                    curated_path = f"s3://{self.config.CURATED_BUCKET}/{args['DATA_SOURCE']}/{args['DATABASE']}-{args['COLLECTION']}-curated/dt={partition_value}/"
                    
                    # Read and transform data
                    df = self.s3_handler.read_partition(raw_path)
                    df = self.data_processor.convert_datetime_columns(df)
                    self.s3_handler.save_to_curated(df, curated_path)
                    
                    processed_count += 1
                    all_paths.append(curated_path)
                    
                except Exception as e:
                    logger.error(f"Error processing partition {partition_value}: {str(e)}")
                    notifier.send_slack_alert(f"⚠️ Error processing partition {partition_value}: {str(e)}")
            
            if not all_paths:
                raise Exception("No partitions were successfully processed")
            
            # Get base path for crawler
            base_path = all_paths[0].rsplit('/dt=', 1)[0] + '/'
            
            logger.info(f"Processed {processed_count} of {len(partitions)} partitions")
            notifier.send_slack_alert(f"✅ Processed {processed_count} of {len(partitions)} partitions for {args['DATABASE']}/{args['COLLECTION']}")
            
            # 3. Create curated database if not exists
            self.glue_handler.create_database_if_not_exists(self.config.CURATED_DATABASE)
            
            # 4. Create or update crawler
            self.glue_handler.create_or_update_crawler(
                database_name=self.config.CURATED_DATABASE,
                crawler_name=self.crawler_name,
                s3_path=base_path
            )
            
            # 5. Run crawler to infer schema and create curated table
            crawler_success = self.glue_handler.run_crawler(crawler_name=self.crawler_name)
            
            if not crawler_success:
                raise Exception(f"Crawler {self.crawler_name} failed to complete successfully")
            
            # 6. Wait additional time to ensure crawler has fully completed
            time.sleep(10)
            
            # 7. Grant permission to the database and the table
            self.lf_manager.grant_permissions(
                database_name=self.config.CURATED_DATABASE,
                table_name=self.curated_table_name
            )
            
            # 8. Analyze the column for tag and apply it to the column in the table
            self.lf_manager.process_and_apply_tags(
                glue_handler=self.glue_handler,
                database_name=self.config.CURATED_DATABASE,
                table_name=self.curated_table_name
            )
            
            # 9. Notify success
            success_message = (
                f":white_check_mark: Bulk processing successful!\n"
                f"• Database: {args['DATABASE']}\n"
                f"• Collection: {args['COLLECTION']}\n"
                f"• Table created: {self.curated_table_name}"
            )
            notifier.send_slack_alert(success_message)
            
            self.job.commit()

        except Exception as e:
            error_msg = f"Job execution failed: {str(e)}"
            logger.error(error_msg)
            notifier.send_slack_alert(f":x: Bulk processing failed: {args['DATABASE']}, {args['COLLECTION']} - {str(e)}")
            raise Exception(error_msg)  # Re-raise to ensure job fails

def main():
    notifier.send_slack_alert(f":arrows_counterclockwise: Starting bulk processing for {args['DATABASE']}/{args['COLLECTION']}...")
    
    # Glue job execution
    glue_context = GlueContext(SparkContext())
    glue_job = GlueJob(glue_context, config)
    glue_job.execute()
    
    notifier.send_slack_alert(f":done: Bulk processing completed for {args['DATABASE']}/{args['COLLECTION']}")

main() 