import json
import urllib.request
import boto3
import time
from urllib.parse import unquote

# Initialize Glue Client
glue_client = boto3.client('glue')

# Slack Bot Token & Channel (Replace with actual values)
SLACK_BOT_TOKEN = 'xoxb-x-x-x'
SLACK_CHANNEL_ID = 'x'
SLACK_USERNAME = 'AWS Lambda - 430493181086'

def trigger_glue_job(object_prefix, partition_value, object_key):
    """
    Triggers an AWS Glue Job with specific file information.
    """
    if not object_prefix or not object_key:
        return {"status": "Error", "message": "Missing required parameters"}

    # Extract database and collection from the object key
    # Example path: mongodb/database-collection/dt=2024-03-20/file_final.json
    try:
        path_parts = object_key.split('/')
        if len(path_parts) >= 2 and path_parts[0] == 'mongodb':
            db_coll = path_parts[1]
            database, collection = db_coll.split('-', 1)
        else:
            return {"status": "Error", "message": f"Invalid object key format: {object_key}"}
    except Exception as e:
        return {"status": "Error", "message": f"Failed to parse database and collection: {str(e)}"}

    job_name = "bigdata-prod-ingestion-mongodb"

    try:
        response = glue_client.start_job_run(
            JobName=job_name,
            Arguments={
                "--PARTITION_VALUE": partition_value,
                "--DATABASE": database,
                "--COLLECTION": collection,
                "--SOURCE_FILE": object_key
            }
        )
        return {
            "status": "Success", 
            "jobName": job_name, 
            "jobRunId": response["JobRunId"],
            "database": database,
            "collection": collection
        }
    except Exception as e:
        return {"status": "Error", "message": str(e), "jobName": job_name}

def lambda_handler(event, context):
    """
    AWS Lambda function entry point.
    """
    try:
        # Extract the first record (S3 event notifications typically contain a single record)
        record = event.get("Records", [{}])[0]
        
        # Extract required event details
        event_source = record.get("eventSource", "Unknown")
        aws_region = record.get("awsRegion", "Unknown")
        event_name = record.get("eventName", "Unknown")
        s3_info = record.get("s3", {})

        bucket_name = s3_info.get("bucket", {}).get("name", "Unknown")
        object_key = s3_info.get("object", {}).get("key", "Unknown")
        object_size = s3_info.get("object", {}).get("size", "Unknown")

        # Decode the object key in case it's URL-encoded
        decoded_object_key = unquote(object_key)

        # Determine object prefix based on "dt=" presence
        if "dt=" in decoded_object_key:
            object_prefix = decoded_object_key.split("dt=")[0].rstrip("/").replace("/", "-")
        else:
            object_prefix = decoded_object_key.replace("/", "-")

        # Extract partition value from the S3 object key
        partition_value = None
        if "dt=" in decoded_object_key:
            partition_value = decoded_object_key.split("dt=")[1].split("/")[0]  # Extract partition date

        # Trigger Glue Job
        glue_job_response = trigger_glue_job(object_prefix, partition_value, decoded_object_key)

        # Construct the message for Slack
        slack_message = (
            f"*{context.function_name}*\n"
            f"> *Event Source*: `{event_source}`\n"
            f"> *AWS Region*: `{aws_region}`\n"
            f"> *Event Name*: `{event_name}`\n"
            f"> *Bucket Name*: `{bucket_name}`\n"
            f"> *Object Key*: `{decoded_object_key}`\n"
            f"> *File Size*: `{object_size}` bytes\n"
            f"> *Object Prefix*: `{object_prefix}`\n"
            f"> *Database*: `{glue_job_response.get('database', 'N/A')}`\n"
            f"> *Collection*: `{glue_job_response.get('collection', 'N/A')}`\n"
            f"> *Glue Job*: `{glue_job_response.get('jobName', 'N/A')}` - `{glue_job_response.get('status')}` (Run ID: `{glue_job_response.get('jobRunId', 'N/A')}`)"
        )

        # Prepare the payload for Slack API
        payload = {
            'channel': SLACK_CHANNEL_ID,
            'text': slack_message,
            'username': SLACK_USERNAME,
            'icon_url': "https://upload.wikimedia.org/wikipedia/commons/thumb/5/5c/Amazon_Lambda_architecture_logo.svg/1200px-Amazon_Lambda_architecture_logo.svg.png"
        }

        # Slack API URL
        url = 'https://slack.com/api/chat.postMessage'

        # Set up headers with the Slack bot token
        headers = {
            'Authorization': f'Bearer {SLACK_BOT_TOKEN}',
            'Content-Type': 'application/json'
        }

        # Convert payload to JSON
        data = json.dumps(payload).encode('utf-8')

        # Send request to Slack
        request = urllib.request.Request(url, data=data, headers=headers)
        urllib.request.urlopen(request)

        # Return success response
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Notification sent to Slack successfully!'})
        }

    except Exception as e:
        error_message = f"Lambda function failed with error: {str(e)}"

        # Prepare Slack error message
        error_payload = {
            'channel': SLACK_CHANNEL_ID,
            'text': f":warning: *Lambda Execution Error*\n{error_message}",
            'username': SLACK_USERNAME,
        }

        try:
            # Send the error message to Slack
            error_data = json.dumps(error_payload).encode('utf-8')
            request = urllib.request.Request(url, data=error_data, headers=headers)
            urllib.request.urlopen(request)
            print("[ERROR] Slack error notification sent.")
        except Exception as slack_error:
            print(f"[ERROR] Failed to send error notification to Slack: {str(slack_error)}")

        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'Lambda function execution failed.', 'error': str(e)})
        }
