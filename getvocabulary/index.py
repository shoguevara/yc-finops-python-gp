from botocore.exceptions import ClientError
import os
import requests
import logging
import boto3
import json
import csv

# Configuration - Logging
logging.getLogger().setLevel(logging.INFO)

# Variables
config = {
    'cloud_id'         : os.environ['CLOUD_ID'],
    'bucket_name'      : os.environ['BUCKET'],
    'aws_key'          : os.environ['AWS_KEY'],
    'aws_secret'       : os.environ['AWS_SECRET'],
    'dict_file_object' : os.environ['FILE_NAME']
}

token_url         = 'http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token'
token_headers     = {'Metadata-Flavor': 'Google'}
list_clusters_url = 'https://mdb.api.cloud.yandex.net/managed-greenplum/v1/clusters'
list_folder_url   = 'https://resource-manager.api.cloud.yandex.net/resource-manager/v1/folders'

clusters          = {}

# State - Setting up S3 client
s3 = boto3.client('s3',
    endpoint_url            = 'https://storage.yandexcloud.net',
    aws_access_key_id       = config['aws_key'],
    aws_secret_access_key   = config['aws_secret'] 
)

# Function - Get token
def get_token():
    response = requests.get(token_url, headers=token_headers)
    content = response.json()
    return content['access_token']

# Function - List clusters
def find_clusters(folder_id, token):
    request_headers = {'Authorization': 'Bearer {}'.format(token)}
    params = {'folderId': folder_id}

    response = requests.get(list_clusters_url, headers=request_headers, params=params)
    content = response.json()

    if "clusters" in content:
        for c in content["clusters"]:
            if 'labels' in c:
                clusters[c['id']] = c['labels']

# Function - Get folders
def get_folders(cloud_id, token):
    folders = []
    request_headers = {'Authorization': 'Bearer {}'.format(token)}
    params = {'cloudId': cloud_id}

    response = requests.get(list_folder_url, headers=request_headers, params=params)
    content = response.json()

    for f in content['folders']:
        folders.append(f['id'])
    
    return(folders)

# Function - Process folders
def process_folders(folders, token):
    for f in folders:
        find_clusters(f, token)

# Function - Upload JSON file
def put_json_file(clusters):
    json_data = json.dumps(clusters)
    try:
        s3.put_object(Bucket=config['bucket_name'], Key=config['dict_file_object'], Body=json_data)
        logging.info("Dictionary file was written")
        return True
    except ClientError as e:
        logging.error("Dictionary file write failed: {}".format(e))
        return None

# Function - Prepare and upload CSV file
def put_csv_file(clusters):
    prefix = 'label.user_labels.'
    header = ['clusterId'] + sorted(set(subkey for value_dict in clusters.values() for subkey in value_dict.keys()))
    header_full = ['clusterId']

    for key in header[1:]:
        header_full.append(prefix+key)
    
    with open('/tmp/output.csv', 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(header_full)

        for key, value_dict in clusters.items():
            row = [key] + [value_dict.get(subkey, '') for subkey in header[1:]]
            csv_writer.writerow(row)

    logging.info("CSV file has been written")

    try:
        with open('/tmp/output.csv', 'rb') as csvfile:
            s3.upload_fileobj(csvfile, config['bucket_name'], config['dict_file_object'])
        logging.info("Dictionary file was uploaded")
        return True
    except ClientError as e:
        logging.error("Dictionary file upload failed: {}".format(e))
        return None

# Main handler
def handler(event, context):
    token = get_token()
    folders =  get_folders(config['cloud_id'], token)
    process_folders(folders, token)
    put_csv_file(clusters)