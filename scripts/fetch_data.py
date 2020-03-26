import os

import boto3
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import DictCursor

from typing import Any, Dict, List, Optional, Tuple

load_dotenv()

DB_CREDENTIALS = {
    'user': os.environ['PGUSER'],
    'password': os.environ['PGPASSWORD'],
    'host': os.environ['PGHOST'],
    'database': os.environ['PGDATABASE']
}

# bucket and folder urls
BUCKET = os.environ['AWS_BUCKET']
DATA_DIR = './data/'

# create the client object
client = boto3.client(
    's3',
    region_name=os.environ['AWS_REGION'],
    aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
    aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY']
)

def download_coughs():
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET):
        for obj in page['Contents']:
            key = obj['Key']
            tmp_dir =  DATA_DIR + '/'.join(key.split('/')[0:-1])
            if not os.path.exists(tmp_dir):
                os.makedirs(tmp_dir)
            else:
                filepath = f'{tmp_dir}/{key.split("/")[-1]}'
                if os.path.isfile(filepath):
                    continue
                print(f"Downloading file to {filepath}...")
                client.download_file(BUCKET, key, filepath)

def fetch_coughs_from_db(conn) -> List[dict]:
    """Fetch coughs from the DB

    :param conn: database connection {class}
    :return: the cough metadata {List[dict]}
    """
    sql = """
    SELECT cough_files.filename, coughs.questions
    FROM coughs
    INNER JOIN cough_files ON coughs.id=cough_files.cough_id
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    return [dict(r) for r in rows]

if __name__ == "__main__":
    download_coughs()
    with psycopg2.connect(**DB_CREDENTIALS, cursor_factory=DictCursor) as conn:
        print(fetch_coughs_from_db(conn))
