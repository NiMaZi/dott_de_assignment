import datetime
from google.cloud import storage

def check_new_file(bucket_name, prefix):
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    curr_ts = datetime.datetime.now()
    
    blobs = list(bucket.list_blobs(prefix = prefix + 'scheduler_log'))

    if not len(blobs):
        blob = bucket.blob(prefix + 'scheduler_log')
        blob.upload_from_string(curr_ts.strftime("%Y-%m-%d-%H-%M-%S"))

    for blob in client.list_blobs(bucket_name, prefix = prefix):
        print(str(blob))