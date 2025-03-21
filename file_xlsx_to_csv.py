import pandas as pd
from google.cloud import storage
from io import BytesIO

def xlsx_to_csv(bucket_name):
    """
    Triggered by a Cloud Storage event.
    Converts an XLSX file to CSV and uploads it to a new location.

    """
    #bucket_name = event['bucket']
    #file_name = event['name']
    
    # Get the source and target paths
    #source_path = f"gs://case_boticario_paulolima/*.xlsx"
    #target_path = f"gs://case_boticario_paulolima/{file_name.replace('.xlsx', '.csv')}"
    
    # Initialize the Cloud Storage client
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = storage_client.list_blobs(bucket_name)
    print(blobs)
    #blob = bucket.blob(file_name)
    
    for blob in blobs :
        blob_name = bucket.blob(blob.name)
        target_path = f"gs://{blob.bucket}/{blob.name}"

        # Download the XLSX file as bytes
        blob_result = blob.download_as_bytes()
        
        # Convert the XLSX file to a Pandas DataFrame
        df = pd.read_excel(target_path)
        
        # Convert the DataFrame to a CSV string
        csv_data = df.to_csv(index=False)
        
        # Create a new blob for the CSV file
        csv_blob = bucket.blob(blob_name.name.replace('.xlsx', '.csv'))
        
        # Upload the CSV data to Cloud Storage
        csv_blob.upload_from_string(csv_data, content_type='text/csv')
        
        print(f"XLSX file converted and uploaded to: {target_path}")

xlsx_to_csv("case_boticario_paulolima")