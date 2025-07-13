import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import pandas as pd

class DataLoader:
    def __init__(self, df, root_folder, api_type, bucket_name):
        self.df = df
        self.root_folder=root_folder
        self.api_type=api_type
        self.bucket_name = bucket_name
    
    def load_to_gcs(self):
        if self.api_type == 'transactions':
            self.df['createdAt'] = pd.to_datetime(self.df['createdAt'])
            unique_dates = self.df['createdAt'].dt.date.unique()
            schema = pa.schema([
                ('createdAt', pa.timestamp('us', tz='UTC')),
                ('id', pa.string()),
                ('chargeId', pa.string()),
                ('grossAmount', pa.float64()),     
                ('currencyCode', pa.string()),     
                ('netAmount', pa.float64()),       
                ('shopID', pa.string()),           
                ('shopDomain', pa.string()),       
                ('shopifyFee', pa.float64()),     
                ('cursor', pa.string())      
            ]) 
        elif self.api_type == 'events':
            self.df['occurredAt'] = pd.to_datetime(self.df['occurredAt'])
            unique_dates = self.df['occurredAt'].dt.date.unique()
            schema = pa.schema([
                ('eventType', pa.string()),
                ('occurredAt', pa.timestamp('us', tz='UTC')),
                ('shopDomain', pa.string()),
                ('shopName', pa.string()),
                ('shopId', pa.string()),
                ('appName', pa.string()),
                ('reason', pa.string()),
                ('chargeAmount', pa.float64()),
                ('currencyCode', pa.string()),
                ('billingOn', pa.string()),
                ('chargeId', pa.string()),
                ('chargeName', pa.string()),
                ('cursor', pa.string())
            ])
        else:
            raise ValueError(f"Unsupported api_type: {self.api_type}")

        for current_date in unique_dates:
            date_str = current_date.strftime('%Y-%m-%d')
            print(f"Processing date: {date_str}")
            
            # Filter DataFrame for the current date
            if self.api_type == 'events':
                date_filtered_df = self.df[self.df['occurredAt'].dt.date == current_date]
            if self.api_type == 'transactions':
                date_filtered_df = self.df[self.df['createdAt'].dt.date == current_date]

            # Convert filtered DataFrame to PyArrow Table
            table = pa.Table.from_pandas(date_filtered_df, schema=schema)

            # Write table to Parquet format in memory
            parquet_buffer = BytesIO()
            pq.write_table(table, parquet_buffer)
            parquet_buffer.seek(0)

            # Define GCS path for the corresponding date
            gcs_path = f"{self.root_folder}/dt={date_str}"
            file_name = f"{self.api_type}_{date_str}.parquet"
            full_gcs_path = f"{gcs_path}/{file_name}"

            # Upload to GCS
            gcs_hook = GCSHook()
            gcs_hook.upload(
                bucket_name=self.bucket_name,
                object_name=full_gcs_path,
                data=parquet_buffer.getvalue(),
                mime_type='application/octet-stream'
            )

            print(f"Uploaded {file_name} to {full_gcs_path}")
                
