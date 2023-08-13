import logging

MODULES = [
    'requests',
    'urllib3',
    'boto',
    'botocore',
    'botocore.credentials',
    'boto3',
    's3transfer',
    's3transfer.utils',
    's3transfer.tasks',
    'rasterio',
]
for module in MODULES:
    logging.getLogger(module).setLevel(logging.WARNING)

logging.getLogger('airflow.task').setLevel(logging.DEBUG)

logger = logging.getLogger('treebase')
logger.setLevel(logging.DEBUG)
