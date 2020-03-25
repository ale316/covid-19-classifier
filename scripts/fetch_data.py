import os

import boto3
from dotenv import load_dotenv

load_dotenv()

# create the client object
client = boto3.client(
    's3',
    region_name=os.environ['AWS_REGION'],
    aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
    aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY']
)

# bucket and folder urls
bucket = os.environ['AWS_BUCKET']
data_dir = './data/'

def download_coughs():
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket):
        for obj in page['Contents']:
            key = obj['Key']
            tmp_dir =  data_dir + '/'.join(key.split('/')[0:-1])
            if not os.path.exists(tmp_dir):
                os.makedirs(tmp_dir)
            else:
                filepath = f'{tmp_dir}/{key.split("/")[-1]}'
                print(f"Downloading file to {filepath}...")
                client.download_file(bucket, key, filepath)

if __name__ == "__main__":
    download_coughs()
