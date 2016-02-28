from __future__ import division
import argparse
import requests
import bs4
import json
from multiprocessing import Pool
import boto3
import md5
from boto3.dynamodb.conditions import Key
import datetime

dynamo = boto3.client('dynamodb', region_name='us-east-1')

try:
    dynamo.describe_table(TableName='manual_moderations')
except Exception, e:
    if "not found: Table" in str(e):
        print "DynamoDB table does not exist (exited)"
        exit(1)

moderations_table = boto3.resource('dynamodb').Table('manual_moderations')


def parse_args():
    parser = argparse.ArgumentParser(description='Mantika moderation parser')
    parser.add_argument('--category', metavar='CAT', help='Category which the items belong to', required=True)
    return parser.parse_args()

def query_meli(title):
    payload = [{'title': title}]

    r = requests.post('https://api.mercadolibre.com/sites/MLA/category_predictor/predict', data=json.dumps(payload), headers={'content-type': 'application/json'})
    if r.status_code != 200:
        return ''
    return r.json()[0]['id']

def query_mantika(title):
    payload = {'title': title}

    r = requests.post('http://ec2-54-86-131-70.compute-1.amazonaws.com:8080/prediction', data=json.dumps(payload), headers={'content-type': 'application/json'})
    if r.status_code != 200 or not r.json()['scores']:
        print 'Mantika error or empty scores for title %s' % title
        return 'NULL'
    return r.json()['scores'][0]['id']

def get_dynamo_items(category):
    response = moderations_table.query(
        KeyConditionExpression=Key('category').eq(category)
    )
    if 'Items' in response:
        if 'LastEvaluatedIndex' in response:
            print 'Query reached limit, need to query again (pending)'

        return response['Items']
    else:
        return []

def save_item(item):
    print item
    resp = moderations_table.put_item(
            Item=item
    )

def score_items(options):
    p = Pool(2)

    items = get_dynamo_items(options.category)
    total = mantika_ok_total = meli_ok_total = 0
    current_isodate = datetime.datetime.utcnow().isoformat()


    for item in items:
        item_data = json.loads(item['id'])
        title = item_data['title']

        meli_query =  p.apply_async(query_meli, (title,))
        mantika_query = p.apply_async(query_mantika, (title,))
        meli_result = meli_query.get(timeout=10)
        mantika_result = mantika_query.get(timeout=10)

        item['mantika'] = mantika_result
        item['meli'] = meli_result
        item['meli_ok'] = 0
        item['mantika_ok'] = 0

        if mantika_result == item['category']:
            item['mantika_ok'] = 1
            mantika_ok_total+=1
        if meli_result == item['category']:
            item['meli_ok'] = 1
            meli_ok_total+=1

        item['last_run'] = current_isodate
        save_item(item)

        if mantika_result != item['category'] and meli_result != item['category']:
            continue

        total+=1

    print "Mantika's model is %f different than Meli's" % (mantika_ok_total/total - meli_ok_total/total)


if __name__ == '__main__':
    score_items(parse_args())
