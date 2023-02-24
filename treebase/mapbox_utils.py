import os
import requests
import time
import subprocess
import logging

import boto3


class URLS:
    LIST_TILESETS = 'https://api.mapbox.com/tilesets/v1/treebase'
    UPLOAD_CREDENTIALS = 'https://api.mapbox.com/uploads/v1/treebase/credentials'
    CREATE_UPLOAD = 'https://api.mapbox.com/uploads/v1/treebase'
    UPLOAD_STATUS = 'https://api.mapbox.com/uploads/v1/treebase/'


def tileset_suffix():
    return os.environ.get('MAPBOX_TILESET_SUFFIX', '')

def auth():
    return dict(access_token=os.environ['MAPBOX_ACCESS_TOKEN'])


def upload_tileset(filename, tileset, name, suffixed=False):
    if suffixed:
        tileset += tileset_suffix()
        name += ' ' + tileset_suffix()
    creds = requests.get(URLS.UPLOAD_CREDENTIALS, params=auth()).json()
    s3_client = boto3.client(
        's3',
        aws_access_key_id=creds['accessKeyId'],
        aws_secret_access_key=creds['secretAccessKey'],
        aws_session_token=creds['sessionToken'],
        region_name='us-east-1',
    )
    s3_client.upload_file(
        filename, creds['bucket'], creds['key']
    )
    data = dict(
        tileset=tileset,
        url=creds['url'],
        name=name
    )
    upload = requests.post(URLS.CREATE_UPLOAD, params=auth(), json=data).json()
    print(upload)
    assert not upload.get('error')
    while True:
        status = requests.get(URLS.UPLOAD_STATUS + upload['id'], params=auth()).json()
        assert not status.get('error')
        print('{complete} / {progress}'.format(**status))
        if status['complete']:
            break
        time.sleep(10)


def fetch_tilesets():
    return requests.get(URLS.LIST_TILESETS, params=auth()).json()


def run_tippecanoe(*args):
    try:
        cmd = ['tippecanoe', '--force', *args]
        out = subprocess.check_output(cmd).decode('utf8')
        logging.debug('TC SUCCESS %s:\n%s', cmd, out)
        return True
    except subprocess.CalledProcessError as e:
        msg = b'\n'.join(filter(lambda x: x, (e.stderr, e.stdout, e.output)))
        logging.warning('TC FAILED %s:\n%s - %s', cmd, msg.decode('utf8'), str(e))
        return False