from airflow.providers.google.cloud.hooks.gcs import GCSHook
import pandas as pd
import re
from io import BytesIO
from datetime import datetime

class GCSDataChecker:
    def __init__(self, bucket_name, folder_path_prefix, gcs_conn_id='google_cloud_default'):
        self.bucket_name = bucket_name
        self.folder_path_prefix = folder_path_prefix
        self.gcs_hook = GCSHook(gcs_conn_id=gcs_conn_id)

    def get_latest_dt_folder(self):
        blobs=self.gcs_hook.list(bucket_name=self.bucket_name, prefix=self.folder_path_prefix)
        print(f'Bucket is {self.bucket_name}')
        print(f'The path is {self.folder_path_prefix}')

        dt_folders = []
        current_date_str = datetime.now().strftime('%Y-%m-%d')

        for blob in blobs:
            match = re.search(r'dt=(\d{4}-\d{2}-\d{2})/', blob)
            if match:
                folder_date = match.group(1)
                if folder_date != current_date_str:
                    dt_folders.append(folder_date)

        if not dt_folders:
            print("No 'dt' folders found.")
            return None  # Return None if no folders are found

        latest_dt = max(dt_folders)
        print(f"Latest 'dt' folder: {latest_dt}")
        return latest_dt
    
    def fetch_files_from_latest_df(self):
        latest_dt = self.get_latest_dt_folder()
        if latest_dt is None:
            return pd.DataFrame()
        print(latest_dt)
        
        full_path = f"{self.folder_path_prefix}/dt={latest_dt}/"
        print(full_path)
        blobs = self.gcs_hook.list(bucket_name=self.bucket_name, prefix=full_path)

        data_frames = [
            pd.read_parquet(BytesIO(self.gcs_hook.download(bucket_name=self.bucket_name, object_name=blob_name)))
            for blob_name in blobs
        ]
        
        if not data_frames:
            print("No parquet file found. Returning empty DataFrame")
            return pd.DataFrame()

        return pd.concat(data_frames, ignore_index=True)
