from datetime import datetime
# import datetime
import json
import time
import os
from dotenv import load_dotenv
import requests as rq
from botocore.exceptions import ClientError
import boto3

def load_files_to_s3(filepath_base,s3filepath_base,api_keys,endswith,remove_local = False,):
    # filepath_base = 'data'
    filenames = os.listdir(filepath_base)
    # filenames = [f for f in filenames if f.endswith('.json') and filenames]
    print(filenames)
    # print(api_keys)

    s3_client = boto3.client(
        's3',
        aws_access_key_id = api_keys['Access_key_ID'],
        aws_secret_access_key = api_keys['Secret_access_key'],
        region_name = 'eu-north-1'
    )
    for filename in filenames:
        if filename.endswith(endswith):
            try:
                # filepath = os.path.join(filepath_base,filename)
                # s3_path = os.path.join(s3filepath_base,filename)
                filepath = filepath_base + '/' + filename
                s3_path = s3filepath_base + '/' + filename
                print(filepath,s3_path)
                s3_client.upload_file(
                    Filename=filepath,   # local path
                    Bucket=api_keys['AWS_BUCKET_NAME'],             # target bucket
                    Key=s3_path,# object key (path in bucket)
                )
                if remove_local:
                    os.remove(filepath)
                print("Upload succeeded")
            except ClientError as err:
                print(f"Upload failed: {err}")


    print(s3_client)
    return

def load_logs_csv(filepath_base,s3filepath_base,api_keys,remove_local = False, endswith = 'logs.csv'):
    # filepath_base = 'logs'
    filenames = os.listdir(filepath_base)
    # filenames = [f for f in filenames if f.endswith('.json') and filenames]
    print(filenames)
    # print(api_keys)

    s3_client = boto3.client(
        's3',
        aws_access_key_id = api_keys['Access_key_ID'],
        aws_secret_access_key = api_keys['Secret_access_key'],
        region_name = 'eu-north-1'
    )
    for filename in filenames:
        if filename.endswith(endswith):
            try:
                # filepath = os.path.join(filepath_base,filename)
                # s3_path = os.path.join(s3filepath_base,filename)
                filepath = filepath_base + '/' + filename
                s3_path = s3filepath_base + '/' + filename
                print(filepath,s3_path)
                s3_client.upload_file(
                    Filename=filepath,   # local path
                    Bucket=api_keys['AWS_BUCKET_NAME'],             # target bucket
                    Key=s3_path,# object key (path in bucket)
                )
                if remove_local:
                    os.remove(filepath)
                print("Upload succeeded")
            except ClientError as err:
                print(f"Upload failed: {err}")


    # print(s3_client)
    return

if __name__ == '__main__':
    print('hi')
