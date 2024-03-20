import pandas as pd
from sodapy import Socrata
import boto3  # Import boto3 for S3 interaction

def lambda_handler(event, context):
    # Socrata API access
    client = Socrata("data.cityofnewyork.us", None)
    results = client.get("h9gi-nx95", where ="crash_date > '2024-03-10T00:00:00'", order="crash_date ASC", limit=50000)
    results_df = pd.DataFrame.from_records(results)

    # Handle potential empty DataFrame scenario
    if results_df.empty:
        print("No data retrieved from Socrata API.")
        return  # Early exit if no data

    bucket_name = "nyc-data-landing-zone"
    s3_client = boto3.client("s3", aws_access_key_id="your-access-key", aws_secret_access_key="your-secret-key")

    csv_buffer = results_df.to_csv(index=False, encoding="utf-8")
    s3_client.put_object(Body=csv_buffer, Bucket=bucket_name, Key="nyc_crash_data.csv")
    print(f"Number of records :f {len(results_df)}")

    return {
        "statusCode": 200,
        "body": f"Successfully saved {len(results_df)} rows of crash data to S3!"
    }

# Example usage (if running the script outside of AWS Lambda)
if __name__ == "__main__":
    lambda_handler(None, None)
