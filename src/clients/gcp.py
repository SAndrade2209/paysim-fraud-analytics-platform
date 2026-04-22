from google.cloud import storage

def get_bucket(bucket_name):
    client = storage.Client()
    return client.bucket(bucket_name)