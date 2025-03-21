#import pandas as pd
from google.cloud import storage
from io import BytesIO

def xlsx_to_csv(event, context):
    """
    Triggered by a Cloud Storage event.
    Converts an XLSX file to CSV and uploads it to a new location.

    """
    bucket_name = event['bucket']
    file_name = event['name']
    
    # Get the source and target paths
    source_path = f"gs://case_boticario_paulolima/*.xlsx"
    target_path = f"gs://case_boticario_paulolima/{file_name.replace('.xlsx', '.csv')}"
    
    # Initialize the Cloud Storage client
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    
    # Download the XLSX file as bytes
    blob_result = blob.download_as_bytes()
    
    # Convert the XLSX file to a Pandas DataFrame
    df = pd.read_excel(BytesIO(blob_result))
    
    # Convert the DataFrame to a CSV string
    csv_data = df.to_csv(index=False)
    
    # Create a new blob for the CSV file
    csv_blob = bucket.blob(file_name.replace('.xlsx', '.csv'))
    
    # Upload the CSV data to Cloud Storage
    csv_blob.upload_from_string(csv_data, content_type='text/csv')
    
    print(f"XLSX file converted and uploaded to: {target_path}")