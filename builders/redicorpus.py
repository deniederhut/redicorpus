#!/usr/bin/env python
"""
High temporal resolution, web-based corpus building and querying
"""

import datetime
import glob
import json
import logging
from nltk import ngrams, snowball, wordnet, word_tokenize
import os
import pymongo
import re
from redicorpus.celery import app
import requests
import yaml

mongo = pymongo.MongoClient()
snowball = snowball.Englishstemmer()
wordnet = wordnet.WordNetLemmatizer()
logging.basicConfig(filename = RCDIR + '/etc/redicorpus.log', level = logging.INFO, format = '%(asctime)s %(message)s')

class comment(object):
    """A single communicative event"""

    def __init__(self, data):
        if type(data) != dict:
            raise TypeError
        self.data = data
        if 'raw' not in self:
            raise ValueError("Comments need a 'raw' value field")
        self.data['tokens']['strings'] = word_tokenize(self.data['raw'])
        self.data['tokens']['stems'] = [snowball.stem(token) for token in self.data['strings']]
        self.data['tokens']['lemmas'] = [wordnet.lemmatize(token) for token in self.data['strings']
        if 'date' not in self.data:
            self.data['date'] = datetime.datetime.now()
        if 'child_id' not in self.data:
            self.data['child_id'] = []
        if 'polarity' not in self.data:
            self.data['polarity'] = {}
        self.data['counted'] = False

    @app.task
    def insert(self):
        collection = pymongo.collection.Collection(self.data['source'], 'comments')
        result = collection.insert_one(self.data)
        if result.inserted_count == 1:
            self.count.apply_async(queue='counts')

    @app.task
    def count(self, grams=(1,2,3)):
        database = self.data['source']
        for i in grams:
            collection = pymongo.collection.Collection(database, str(i))
            _id = datetime.datetime(self.data['date'].year,
                                    self.data['date'].month,
                                    self.data['date'].day)
            for token_type in self.data['tokens'].keys():
                for gram in ngrams(self.data['tokens'][token_type], i):
                    result = collection.update_one({'_id' : _id},
                    {'$inc' : {token_type : {gram : {'frequency' : 1}}},
                    {'$inc' : {token_type : {'_total' : 1}}},
                    {'$push' : {token_type : {gram : {'_id' : self.data['_id']}}}}},
                    upsert = True)
        return True

class body(dict):
    """All of the frequency counts for a day"""

    def __init__(self, data):
        self = data

class map(dict):
    """ """

    def __init__(self):

    def test():

    def control():

def read(database, collection, _id):

    # class document(object):
#     """A page with metadata"""

#     def __init__(self):
#         self.data = []

#     def build(self, content):
#         """Build document from structured dict"""
#         if type(data) != dict:
#             raise TypeError('Content is not json-like')
#         self._id = content['_id']
#         self.source = content['source']
#         self.created = content['created']
#         self.requested = content['requested']
#         self.data = content['data']

#     def write_json(self):
#         """Write document to file as json"""
#         datename = create_datename(self.created)
#         try:
#             os.makedirs(RCDIR + "/pages/" + datename)
#         except OSError:
#             if not os.path.isdir(RCDIR + "/pages/" + datename):
#                 raise
#         with open(RCDIR + '/pages/' + datename + '/' + self._id + '.json','w') as f:
#             json.dump({'_id' : self._id, 'source' : self.source,
#                        'created' : self.created, 'requested' : self.requested
#                        'data' : self.data}, f)

#     def read_json(fp):
#         """Read document from json by filepath"""
#         with open(fp,'r') as f:
#             return document.build(json.load(f))

#     @staticmethod
#     def read_json_from_date(date, num_days=0, all_data=False):
#         """
#         Read page(s) by a datetime object and integer of days before or after.
#         Returns a generator of loaded json pages
#         E.g. ...from_date(datetime.datetime(2000,12,25), -12) for the twelve
#         days of Christmas. To return all json pages, set all_data=True
#         """
#         if all_data == True:
#             for fp in glob.glob(RCDIR + '/pages/*/*.json'):
#                 yield document.read_json(fp)
#         else:
#             if num_days == 0:
#                 datename = create_datename(date)
#                 for fp in glob.glob(RCDIR + '/pages/' + datename + '/*.json'):
#                     yield document.read_json(fp)
#             if num_days != 0:
#                 for i in range(0:num_days):
#                     datename = create_datename(datetime.date(date) + datetime.timedelta(i))
#                     for fp in glob.glob(RCDIR + '/pages/' + date_name + '/*.json'):
#                         yield document.read_json(fp)
