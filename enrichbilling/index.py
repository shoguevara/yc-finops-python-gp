import pandas as pd
import json
import csv
import os
import boto3
import sys
from io import StringIO
import requests

def getgpclusterhosts(clusterid,iamtoken):
    segmenthosts = requests.get("https://mdb.api.cloud.yandex.net/managed-greenplum/v1/clusters/%s/segment-hosts" % (clusterid), headers={'Authorization': 'Bearer %s'  % iamtoken})
    masterhosts = requests.get("https://mdb.api.cloud.yandex.net/managed-greenplum/v1/clusters/%s/master-hosts" % (clusterid), headers={'Authorization': 'Bearer %s'  % iamtoken})
    headlessmaster = masterhosts.json()['hosts']
    headlesssegment = segmenthosts.json()['hosts']
    masterbody = pd.json_normalize(headlessmaster).rename(columns={'name': 'resource_id'})
    segmentbody = pd.json_normalize(headlesssegment).rename(columns={'name': 'resource_id'})
    selected_cols = ['resource_id', 'clusterId']
    masterbody = masterbody[selected_cols]
    segmentbody = segmentbody[selected_cols]
    hosts = pd.concat([masterbody, segmentbody], axis=0)
    return hosts

def getcsvfroms3(bucket_name,object_key):
    session = boto3.session.Session()
    s3 = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net'
    )
    csv_obj = s3.get_object(Bucket=bucket_name, Key=object_key)
    body = csv_obj['Body']
    csv_string = body.read().decode('utf-8')
    df1 = pd.read_csv(StringIO(csv_string))
    return df1

def intermhostslist(hosts,voc):
    result = pd.merge(hosts,voc,on='clusterId',how='left')
    return result

def transform(inputcsv,intermhostslist):
    df1 = pd.DataFrame(inputcsv)
    merged_df = df1.merge(intermhostslist, on='resource_id', how='left')
    merged_df = df1.merge(intermhostslist, on='resource_id', how='left', suffixes=('', '_intermhostslist'))
    merged_df = merged_df.loc[:, ~merged_df.columns.duplicated()]
    merged_df = merged_df.loc[:, ~(merged_df.columns.str.endswith('_intermhostslist') & merged_df.columns.str[:-15].isin(df1.columns))]
    return merged_df

def saveresultingcsv(bucket_name,object_key,df):
    session = boto3.session.Session()
    s3 = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net'
    )
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
    csv_string = csv_buffer.getvalue().encode('utf-8-sig')
    s3.put_object(Body=csv_string, Bucket=bucket_name, Key=object_key)

def handler(event, context):
    bucket_name = event['messages'][0]['details']['bucket_id']
    object_key = event['messages'][0]['details']['object_id']
    folder = event['messages'][0]['event_metadata']['folder_id']
    iamtoken = context.token['access_token']    
    vocbucket = os.environ.get('VOCBUCKET')
    vocobject = os.environ.get('VOCOBJECT')
    voc = getcsvfroms3(vocbucket,vocobject)
    hosts = getgpclusterhosts(voc['clusterId'][0],iamtoken)
    intermhosts = intermhostslist(hosts,voc)
    billingcsv = getcsvfroms3(bucket_name,object_key)
    transformedcsv = transform(billingcsv,intermhosts)

    for index, row in voc[1:].iterrows():
        cluster_id = row['clusterId']
        hosts = getgpclusterhosts(cluster_id,iamtoken)    
        intermhosts = intermhostslist(hosts,voc)
        intertransformedcsv = transform(billingcsv,intermhosts)
        transformedcsv = transformedcsv.combine_first(intertransformedcsv)
    saveresultingcsv(os.environ.get('RESLUTBUCKET'),object_key,transformedcsv)
    return {
        'statusCode': 200,
        'body': 'Success!',
    }
