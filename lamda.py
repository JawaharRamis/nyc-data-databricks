import json
import pandas as pd
from sodapy import Socrata
import boto3 
from botocore.exceptions import ClientError
from urllib3.exceptions import HTTPError  

s3_client = boto3.client("s3")

def lambda_handler(event, context):
    print("creating secrets session")
   
    secret_name = event.get("secret -name")
    region_name = event.get("region-name")
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        # Retrieve secrets securely
        secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret = json.loads(secret_value_response['SecretString'])

    except ClientError as e:
        print(f"Error retrieving secrets: {e}")
        raise  # Re-raise for Lambda to handle
    
    socrata_domain = event.get("domain")
    api_key = "TDZkrnd7Wnm6YN8fhneT2UjqW"
    timeout = int(event.get("timeout"))
    
    try:
        # Connect to Socrata API
        client = Socrata(socrata_domain, api_key, timeout=timeout)

        # Query Socrata API with filters (consider error handling for specific HTTPError codes)
        results = client.get(
            "h9gi-nx95",
            where="crash_date > '2024-03-11T00:00:00'",
            order="crash_date ASC",
            limit=50000
        )
        results_df = pd.DataFrame.from_records(results)

    except HTTPError as e:
        print(f"Error accessing Socrata API: {e}")

    if len(results_df) == 0:
        print("No data retrieved from Socrata API.")
        return

    bucket_name = event.get("bucket-name")
    try:
        # Access credentials securely (consider IAM roles)
        aws_access_key_id = secret['aws-access-key']
        aws_secret_access_key = secret['aws-secret-key']

        # Create S3 client
        s3_client = boto3.client(
            's3'
        )

        # Convert DataFrame to CSV buffer
        csv_buffer = results_df.to_csv(index=False, encoding="utf-8")

        # Upload data to S3 (consider chunking for large datasets)
        s3_client.put_object(Body=csv_buffer, Bucket=bucket_name, Key="nyc_crash_data.csv")
        print(f"Uploaded {len(results_df)} records to S3")

    except ClientError as e:
        print(f"Error uploading data to S3: {e}")
        # Consider retry logic or appropriate response

    return {
        "statusCode": 200,
        "body": f"Successfully processed and uploaded crash data."
    }