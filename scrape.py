import argparse
import requests
import bs4
import json
from multiprocessing import Pool
import boto3
import md5

dynamo = boto3.client('dynamodb', region_name='us-east-1')

try:
    dynamo.describe_table(TableName='manual_moderations')
except Exception, e:
    if "not found: Table" in str(e):
        print "Moderations table not found, creating"
        dynamo.create_table(
           TableName='manual_moderations',
           KeySchema=[
               {
                   'AttributeName': 'category',
                   'KeyType': 'HASH'
               },
               {
                   'AttributeName': 'id',
                   'KeyType': 'RANGE'
               }
           ],
           AttributeDefinitions=[
               {
                   'AttributeName': 'category',
                   'AttributeType': 'S'
               },
               {
                   'AttributeName': 'id',
                   'AttributeType': 'S'
               }

           ],
           ProvisionedThroughput={
               'ReadCapacityUnits': 10,
               'WriteCapacityUnits': 10
           }
        )
        dynamo.get_waiter('table_exists').wait(TableName='manual_moderations')

moderations_table = boto3.resource('dynamodb').Table('manual_moderations')


def parse_args():
    parser = argparse.ArgumentParser(description='Mantika moderation parser')
    parser.add_argument('--url', metavar='URL', help='Url to start scrapping from', required=True)
    parser.add_argument('--category', metavar='CAT', help='Category which the items belong to', required=True)
    parser.add_argument('--workers', type=int, default=8,
                        help='number of workers to use, 8 by default.')
    return parser.parse_args()


def save_item(item):
    print item
    resp = moderations_table.put_item(
            Item=item
    )

def scrape(url, category):
    print 'Begin scrapping on page %s' % url
    response = requests.get(url)
    soup = bs4.BeautifulSoup(response.text)

    next_page = soup.select('.next.follows')

    items =  soup.select('.href-link')

    for item in items:
        title = item.text
        item_id  = json.dumps({'title': title})
        current_item = {
                'id': item_id,
                'category': category
        }
        save_item(current_item)
        print 'Item processed: ', current_item

    if next_page:
        scrape('http://www.alamaula.com' + next_page[0]['href'], category)
    else:
        print 'No next page found on %s, finished scrapping' % url


def start_scrapping(options):
    scrape(options.url, options.category)

if __name__ == '__main__':
    start_scrapping(parse_args())
