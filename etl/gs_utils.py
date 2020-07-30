from google.cloud import storage

def check_new_file(bucket_name, prefix):
    client = storage.Client()
    for blob in client.list_blobs(bucket_name, prefix = prefix):
        print(str(blob))