import datetime
from google.cloud import storage

check_log_path = "~/check_log.txt"

def check_new_file(bucket_name, prefix):
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    curr_ts = datetime.datetime.now()
    
    f = open(check_log_path, 'rw')
    last_log = f.read()

    if not last_log:
        f.write(curr_ts.strftime("%Y-%m-%d-%H-%M-%S"))
        return 1
    
    last_ts = datetime.datetime.strptime(last_log, "%Y-%m-%d-%H-%M-%S")

    f.close()

    for blob in bucket.list_blobs(prefix = prefix):
        if blob.time_created > last_ts:
            return 1
    
    return 0


def check_log():

    curr_ts = datetime.datetime.now()

    f = open(check_log_path, 'rw')
    f.write(curr_ts.strftime("%Y-%m-%d-%H-%M-%S"))
    f.close()