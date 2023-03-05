from contextlib import contextmanager
import os
import boto3


class S3Utils():

    def __init__(self):
        bucket_name = os.environ.get('WASABI_BUCKET_NAME')
        endpoint_url = os.environ.get('WASABI_ENDPOINT_URL')
        aws_access_key_id = os.environ.get('WASABI_ACCESS_KEY_ID')
        aws_secret_access_key = os.environ.get('WASABI_SECRET_ACCESS_KEY')
        aws_region = os.environ.get('WASABI_REGION')

        s3 = boto3.resource('s3',         
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region
        )
        self._bucket: s3.Bucket = s3.Bucket(bucket_name)
        
    def download(self, key, filename):
        return self._bucket.Object(key).download_file(Filename=filename)
    
    def upload(self, filename, key):
        return self._bucket.Object(key).upload_file(Filename=filename)
    
    def exists(self, key):
        try:
            self._bucket.Object(key).load()
            return True
        except Exception:
            return False

    @contextmanager
    def get_or_create(self, key, filename):
        upload = False
        try:
            if self.exists(key):
                self.download(key, filename)
                yield None
            else:
                print('### Creating', filename, '###')
                yield filename
                upload = True
        except Exception as e:
            print('### Error creating', filename, '###', e)
        finally:
            if upload:
                self.upload(filename, key)
