#!/usr/bin/env python
"""
Script to authenticate and scrape from reddit.com/r/AskReddit
"""

# to do - add language to object
# to do - change to PRAW

import datetime
import gzip
import json
import logging
import os
from pymongo import MongoClient
import re
from redicorpus.builders import redicorpus as rc
import requests
import time
import yaml

RCDIR = os.getenv('HOME')

def authorize():
    """Oauth for reddit.com"""
    user_agent = 'linux:redicorpus:v.' + __version__ + ' (by /u/MonsieurDufayel)'
    with open('creds.yaml','r') as f:
        creds = yaml.load(f.read())
    client_auth = requests.auth.HTTPBasicAuth(creds['id'],creds['secret'])
    post_data = {'grant_type':'password', 'username':creds['username'], 'password':creds['password'], 'duration':'permanent'}
    r.status_code = 999
    while response.status_code != 200:
        response = requests.post('https://www.reddit.com/api/v1/access_token', auth=client_auth, data=post_data)
        time.sleep(30)
    return response.json()['token_type'] + ' ' + response.json()['access_token']

def get_listing(date, depth):
    """Retrieve top links from AskReddit"""
    logging.info("Fetching links")
    list_url = 'https://oauth.reddit.com/r/AskReddit/hot/'
    loop_var = ['']
    authorization = authorize()
    for i in depth:
        loop_var.append('?after=t3_')
    for i in loop_var:
        listing = json.loads(requests.get(list_url + i + id36[-1], headers = {
            'User-Agent' : user_agent, 'Authorization' : authorization}).content)
        time.sleep(1)
        for child in listing['data']['children']:
            mongo.askreddit.listings.insert({'_id' : child['data']['id']})

def get_page(id36, comment_id=None):
    """Retrieve comment page by id"""
    url = 'https://oauth.reddit.com/r/AskReddit/comments/' + id36 + '?limit=500&sort=random'
    authorization = authorize()
    if comment != None:
        url += '&comment=' + comment
    r.status_code = 999
    while r.status_code != 200:
        r = requests.get(url,
            headers = {'User-Agent' : user_agent, 'Authorization' : authorization})
        if r.status_code == 200:
            return r.json()
        else:
            time.sleep(1)

def insert_comment(item):
    """Pull info out of raw data and insert to job collection"""
    raw = item['body'].lower()
    links = []
    for match in re.finditer(r'\[(?P<text>.+)\]\((?P=<link>.+)\)',raw):
        raw.replace(match.group(), match.group('text'))
        links.append(match.group('link'))
    comment = rc.comment({
    _id : item['id'],
    source : 'askreddit',
    date : datetime.datetime.fromtimestamp(item['created_utc']),
    thread_id : re.search(r'[a-zA-Z0-9]+$',item['link_id']).group(),
    parent_id : re.search(r'[a-zA-Z0-9]+$',item['parent_id']).group(),
    'author' : item['author'],
    'polarity' : {user : item['controversiality']},
    'raw' : raw
    })
    return comment.insert.apply_async(queue='comments')

def traverse_comments(level):
    """Traverse comment hierarchies depth-first"""
    while len(level) > 0:
        item = level.pop()
        if item['kind'] == 't1':
            rc.comment.build(item)
            children = item['data']['replies']['data']['children']
            if len(children) > 0:
                insert_comment(children)
        elif item['kind'] == 'more':
            thread_id = list(collection.find(
                {'_id':re.search(r'[a-zA-Z0-9]+$',item['data']['parent_id'])},
                {'thread_id':True, '_id':False}
                ))[0]['thread_id']
            page = get_page(thread_id,comment_id=item['data']['id'])
            traverse_comments(page[1]['data']['children'])
        else:
            pass
    return True

def write_page(page):
    """Compress page and write page to file"""
    try:
        os.makedirs(RCDIR + "/raw/askreddit/")
    except OSError:
        if not os.path.isdir(RCDIR + "/raw/askreddit/"):
            raise
    id36 = page[0]['data']['children'][0]['id']
    with gzip.GzipFile(RCDIR + "/raw/askreddit/" + str(id36) + ".json.gz", 'wb') as f:
        f.write(json.dumps(page))
    return True

if __name__ == "__main__":
    logging.info('Starting askreddit getter')
    get_listing(date = NOW, depth = 4)
    for document in mongo.askreddit.listing.find():
        page = get_page(document['_id'])
        a = write_page(page)
        b = traverse_comments(page[1]['data']['children'])
        if a and b:
            mongo.askreddit.listing.remove(document)
    mongo.close()
    logging.info('Finished askreddit getter')
