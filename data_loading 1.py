#upload files from local to S3
!pip install boto3
 
 
#upload transactions.csv file form local to S3 bucket
import logging
import boto3
from botocore.exceptions import ClientError
import os
def upload_file_to_s3(local_file_path, bucket_name, s3_file_key):
    """
    Uploads a file to an S3 bucket.
    :param local_file_path: Path to the local file
    :param bucket_name: Name of the S3 bucket
    :param s3_file_key: S3 object key (path within the bucket)
    """
    # Create an S3 client
    s3_client = boto3.client('s3',
        aws_access_key_id='xxxxxxxxxxxxxxxxxxxxxxxx',       # Replace with your actual access key
        aws_secret_access_key='xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'  # Replace with your actual secret key
    )
    try:
        # Upload file to S3 bucket
        s3_client.upload_file(local_file_path, bucket_name, s3_file_key)
        print(f"File '{local_file_path}' uploaded to bucket '{bucket_name}' as '{s3_file_key}'.")
    except FileNotFoundError:
        print(f"The file '{local_file_path}' was not found.")
    except NoCredentialsError:
        print("Credentials not available.")
    except PartialCredentialsError:
        print("Incomplete credentials provided.")
    except Exception as e:
        print(f"An error occurred: {e}")
#upload transactions.csv file
local_file_path = 'C:/Users/narasimharg/Documents/Snowflake/captone_project/transactions.csv'  # Update with your file path
bucket_name = 'capstonesnara'              # Update with your bucket name
s3_file_key = 'transactions/transactions.csv'            # Update with the desired S3 object key
upload_file_to_s3(local_file_path, bucket_name, s3_file_key)