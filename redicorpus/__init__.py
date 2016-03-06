#!/usr/bin/env python

import json
from pkg_resources import resource_string
import pymongo
from redicorpus.celery import app

# Initializing Mongo Databases
c = pymongo.MongoClient()

# Loading most common ngrams
f = resource_string(__name__, 'data/unigrams.json').decode('utf-8')
unigrams = json.loads(f)
f = resource_string(__name__, 'data/bigrams.json').decode('utf-8')
bigrams = json.loads(f)
f = resource_string(__name__, 'data/trigrams.json').decode('utf-8')
trigrams = json.loads(f)

# Initialize Dictionary and Counters with the top 100 most common
# ngrams. Having these at the start of the numeric index is a lookup
# efficiency concern
if 'Counter' not in c.database_names():
    if 'Dictionary' not in c.database_names():
        for str_type in ['String', 'Stem', 'Lemma']:
            for n, stopword_list in zip([1,2,3], [unigrams, bigrams, trigrams]):
                for ix, term in enumerate(stopword_list):
                    c['Dictionary'][str_type].insert_one({
                        'ix' : ix,
                        'term' : term,
                        'n' : n
                    })
                c['Counter'][str_type].insert_one({
                    'n' : n,
                    'counter' : len(stopword_list) - 1
                })
    else: # if someone deleted the counter database
        raise BaseException("Counter database missing")
else: # if someone deleted the dictionary datamase
    if 'Dictionary' not in c.database_names():
        raise BaseException("Dictionary database missing")

# Set strict write concerns and indices for dictionaries and their counters
for collection in c['Counter'].collection_names():

    # Index counter
    try:
        c['Counter'].create_collection(collection,w=2)
    except pymongo.errors.CollectionInvalid:
        pass
    c['Counter'][collection].create_indexes([
        pymongo.IndexModel(
            [('n', pymongo.ASCENDING)], unique=True, background=False
        )
    ])

    # Index dictionary
    try:
        c['Dictionary'].create_collection(collection, w=2)
    except pymongo.errors.CollectionInvalid:
        pass
    c['Dictionary'][collection].create_indexes([
        pymongo.IndexModel(
            [('ix', pymongo.ASCENDING), ('n', pymongo.ASCENDING)], unique=False, background=False
        ),
        pymongo.IndexModel(
            [('term', pymongo.TEXT)], unique=False, background=False
        )
    ])

# Set slack write concerns and indices for DictLike and ArrayLike records
for collection in c['Comment'].collection_names():

    # Initialize comments
    try:
        c['Comment'].create_collection(collection, j=True)
    except pymongo.errors.CollectionInvalid:
        pass
    c['Comment'][collection].create_indexes([
        pymongo.IndexModel(
            [('_id', pymongo.TEXT)], unique=True, background=True
        ),
        pymongo.IndexModel(
            [('date', pymongo.DESCENDING)], unique=False, background=True
        )
    ])

    # Initialize corpora
    try:
        c['Body'].create_collection(collection, j=True)
    except pymongo.errors.CollectionInvalid:
        pass
    c['Body'][collection].create_indexes([
        pymongo.IndexModel(
            [('term', pymongo.TEXT)], unique=False, background=True
        ),
        pymongo.IndexModel(
            [('date', pymongo.DESCENDING)], unique=False, background=True
        ),
        pymongo.IndexModel(
            [('n', pymongo.ASCENDING)], unique=False, background=True
        )
    ])

# Checking celery

@app.task
def celery_running():
    return True
assert celery_running.apply().result
