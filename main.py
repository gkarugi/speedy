import math
import os
import pprint
from csv import writer
from os.path import dirname, join

import boto3
import mutagen
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

def get_files():
    for page in page_iterator:
        for key in page['Contents']:
            yield key['Key']

files = get_files()
print("here")

success=0
failed=0

def get_duration(fileName):
    file = mutagen.File(fileName)

    if file is not None:
        if file.info is not None:
            length= file.info.length
        else:
            with open('error.log', 'a', newline='', encoding='utf-8') as logf:
                logf.write('An exceptional thing happed - %s : %s' % (fileName,' File info not found'))
                logf.close()
            return
    else:
        with open('error.log', 'a', newline='', encoding='utf-8') as logf:
            logf.write('An exceptional thing happed - %s : %s' % (fileName,'File not found'))
            logf.close()
        return

    List=[key,math.floor(length)]
    
    with open('length.csv', 'a', newline='', encoding='utf-8') as f_object:
        writer_object = writer(f_object)
        writer_object.writerow(List)
        f_object.close()

for key in files:        
    try:
        a,b,c = key.rpartition('/')

        if os.path.isfile("files/" + c) is False:
            with open('files/' + c, 'wb') as f:
                s3_client.download_fileobj(BUCKET_NAME, key, f)
        
        get_duration("files/" + c)
    except Exception as e:
        with open('error.log', 'a', newline='', encoding='utf-8') as logf:
            logf.write('An exceptional thing happed - %s : %s' % (key,e))
            logf.write('\n')
            logf.close()

        with open('failed.log', 'a', newline='', encoding='utf-8') as failed_files:
            failed_files.write(key)
            failed_files.write('\n')
            failed_files.close()

        failed+=1
        pass  # or you could use 'continue'
    success+=1

print(success)
print(failed)

