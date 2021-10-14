import math
import multiprocessing
import os
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

# define worker function
def process(process_name, tasks, results, failed, failed_messages):
    print('[%s] evaluation routine starts' % process_name)

    while True:
        key = tasks.get()
        if not key:
            print('[%s] evaluation routine quits' % process_name)

            # Indicate finished
            results.put(-1)
            break
        else:    
            try:
                a,b,c = key.rpartition('/')

                if os.path.isfile("files/" + c) is False:
                    with open('files/' + c, 'wb') as f:
                        s3_client.download_fileobj(BUCKET_NAME, key, f)
                
                fileName = "files/" + c;
                file = mutagen.File(fileName)

                if file is not None:
                    if file.info is not None:
                        length= file.info.length
                    else:
                        error_message = 'An exceptional thing happed - %s : %s' % (fileName,' File info not found')
                        failed_messages.put(error_message)
                        failed.put(key)
                        return
                else:
                    error_message = 'An exceptional thing happed - %s : %s' % (fileName,'File not found')
                    failed_messages.put(error_message)
                    failed.put(key)
                    return

                List=[key,math.floor(length)]
            except Exception as e:
                error_message = 'An exceptional thing happed - %s : %s' % (key,e)
                failed_messages.put(error_message)
                failed.put(key)
                pass  # or you could use 'continue'       

            # Add result to the queue
            results.put(List)

    return

if __name__ == "__main__":

    # Define IPC manager
    manager = multiprocessing.Manager()

    # Define a list (queue) for tasks and computation results
    tasks = manager.Queue()
    results = manager.Queue()
    failed = manager.Queue()
    failed_messages = manager.Queue()

    # Create process pool with four processes
    num_processes = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(processes=num_processes)
    processes = []

    # Initiate the worker processes
    for i in range(num_processes):

        # Set process name
        process_name = 'P%i' % i

        # Create the process, and connect it to the worker function
        new_process = multiprocessing.Process(target=process, args=(process_name,tasks,results,failed,failed_messages))

        # Add new process to the list of processes
        processes.append(new_process)

        # Start the process
        new_process.start()

        print("retrieving file list")
        # Fill task queue
        files = get_files()
        print("file list retrieved")

        print("adding to queue")
        for key in files:     
            tasks.put(key)
        print("added to queue")

        # Quit the worker processes by sending them -1
        for i in range(num_processes):
            tasks.put(-1)

        # Read calculation results
        num_finished_processes = 0
        while True:
            # Read result
            new_result = results.get()

            # Have a look at the results
            if new_result == -1:
                # Process has finished
                num_finished_processes += 1

                if num_finished_processes == num_processes:
                    break
            else:
                # Output result
                print('Result:' + str(new_result))
