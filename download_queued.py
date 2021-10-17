import os
from functools import partial
from multiprocessing import Pool, cpu_count
from os.path import dirname, join
from queue import Queue

import boto3
from dotenv import load_dotenv

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)
load_dotenv()

ACCESS_KEY = os.environ.get("ACCESS_KEY")
SECRET_KEY = os.environ.get("SECRET_KEY")
REGION = os.environ.get("REGION")

BUCKET_NAME = os.environ.get("BUCKET_NAME")
BUCKET_PREFIX = os.environ.get("BUCKET_PREFIX")

s3_client = boto3.client('s3',
                aws_access_key_id=ACCESS_KEY,
                aws_secret_access_key=SECRET_KEY,
                config=boto3.session.Config(signature_version='s3v4'),
                region_name=REGION)

paginator = s3_client.get_paginator('list_objects')
page_iterator = paginator.paginate(Bucket=BUCKET_NAME, Prefix=BUCKET_PREFIX)

def add_to_tasks():
    file_list = open("files.txt")
    while (line := file_list.readline().rstrip()):
        yield line
    file_list.close()

files = add_to_tasks()

def download_files(path):
    try:
        a,b,c = path.rpartition('/')

        if os.path.isfile("files2/" + c) is False:
            with open('files2/' + c, 'wb') as f:
                s3_client.download_fileobj(BUCKET_NAME, path, f)
    except Exception as e:
        pass

if __name__ == "__main__":
    pool = Pool(cpu_count())
    results = pool.map(download_files, files)
    pool.close()
    pool.join()
