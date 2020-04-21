import json
import os
from subprocess import run

from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import DictCursor

import boto3
from dotenv import load_dotenv # pylint: disable-msg=import-error

load_dotenv()

DB_CREDENTIALS = {
    'user': os.environ['PGUSER'],
    'password': os.environ['PGPASSWORD'],
    'host': os.environ['PGHOST'],
    'database': os.environ['PGDATABASE']
}

# bucket and folder urls
BUCKET = os.environ['AWS_BUCKET']
DATA_DIR = './data'
COUGHS_SUBDIR = 'coughs'
COUGHS_CUT_SUBDIR = 'coughs_cut'
COUGHS_CLEAN_SUBDIR = 'coughs_clean'
COUGHS_FILE = 'coughs.json'
COUGHS_DIR = os.path.join(DATA_DIR, COUGHS_SUBDIR)
COUGHS_CUT_DIR = os.path.join(DATA_DIR, COUGHS_CUT_SUBDIR)
COUGHS_CLEAN_DIR = os.path.join(DATA_DIR, COUGHS_CLEAN_SUBDIR)

# create the client object
CLIENT = boto3.client(
    's3',
    region_name=os.environ['AWS_REGION'],
    aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
    aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY']
)

def download_cough_files():
    print(f'Downloading coughs...')
    paginator = CLIENT.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET):
        for obj in page['Contents']:
            key = obj['Key']
            tmp_dir = os.path.join(DATA_DIR, '/'.join(key.split('/')[0:-1]))
            if not os.path.exists(tmp_dir):
                os.makedirs(tmp_dir)
            else:
                filepath = f'{tmp_dir}/{key.split("/")[-1]}'
                if os.path.isfile(filepath):
                    continue
                CLIENT.download_file(BUCKET, key, filepath)
    print(f'done.')

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

def download_cough_metadata() -> None:
    with psycopg2.connect(**DB_CREDENTIALS, cursor_factory=DictCursor) as conn:
        with open(os.path.join(DATA_DIR, COUGHS_FILE), 'w') as f:
            json.dump(fetch_coughs_from_db(conn), f)

def cut_coughs() -> None:
    if not os.path.exists(COUGHS_CUT_DIR):
        os.mkdir(COUGHS_CUT_DIR)
    print(f'Splitting coughs...')
    # The most important numbers here at the `0.1 1%` at the end.
    # This means to put the cut when there is 0.1s of duration where the audio drops
    # below 1% of the audio volume. For example you can get a lot more cuts by
    # decreasing the duration to 0.05 or increasing the acceptable volume to 3%.
    run(f'for coughfile in {COUGHS_DIR}/*.wav; do '
        f'sox ${{coughfile}} -b 16 {COUGHS_CUT_DIR}/$(basename $coughfile)_cut.wav '
        f'rate 22k channels 1 norm -0.1 silence 1 0.1 0.3% 1 0.1 1% : newfile : restart; done',
        shell=True, check=True)
    print(f'done.')

def _get_sox_field(filename: str, field: str) -> float:
    value_list = run(
        f'sox {filename} -n stats 2>&1 | grep "{field}"',
        shell=True, check=True, capture_output=True) \
        .stdout.decode('utf-8').strip().replace(field, '').split(' ')
    for value in value_list:
        if not value:
            continue
        value = value.strip()
        if 'k' in value:
            float_value = float(value.replace('k', '')) * 1000
        else:
            float_value = float(value)
        return float_value
    raise Exception('no value found')

def clean_coughs() -> None:
    if not os.path.exists(COUGHS_CLEAN_DIR):
        os.mkdir(COUGHS_CLEAN_DIR)
    print(f'Cleaning coughs...')
    for (dirpath, _, filenames) in os.walk(COUGHS_CUT_DIR):
        for filename in filenames:
            if filename.endswith('.wav'):
                path_to_file = os.path.join(dirpath, filename)
                # throw away files that are too small
                if os.path.getsize(path_to_file) < 20000: # 20KB
                    continue
                # throw away gasps (below -10dB)
                peak_level_db = _get_sox_field(path_to_file, 'Pk lev dB')
                if peak_level_db < -10:
                    continue
                # throw away talking (lots of peaks)
                peak_count = _get_sox_field(path_to_file, 'Pk count')
                if peak_count > 8:
                    continue
                run(f'cp {path_to_file} {COUGHS_CLEAN_DIR}', shell=True, check=True)
    print(f'done.')

if __name__ == "__main__":
    download_cough_files()
    download_cough_metadata()
    cut_coughs()
    clean_coughs()
