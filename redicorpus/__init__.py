#!/usr/bin/env python

import json
from pkg_resources import resource_string
import pymongo
from redicorpus.celery import app

# Global variables for __init__
STR_TYPE_LIST = ['String', 'Stem', 'Lemma']

# Initializing MongoDB connection
c = pymongo.MongoClient(j=True)

# Set strict write concerns and indices for dictionaries and their counters
for collection in STR_TYPE_LIST:

    # Create and index counter
    try:
        c['Counter'].create_collection(collection,w=2)
    except pymongo.errors.CollectionInvalid: # if collection already exists
        pass
    c['Counter'][collection].create_indexes([
        pymongo.IndexModel(
            [('n', pymongo.ASCENDING)], unique=True, background=False
        )
    ])

    # Create and index dictionary
    try:
        c['Dictionary'].create_collection(collection, w=2)
    except pymongo.errors.CollectionInvalid:
        pass
    c['Dictionary'][collection].create_indexes([
        pymongo.IndexModel(
            [('ix', pymongo.ASCENDING), ('n', pymongo.ASCENDING)], unique=True, background=False
        ),
        pymongo.IndexModel(
            [('term', pymongo.TEXT)], unique=False, background=False
        )
    ])

# Set slack indexing for DictLike and ArrayLike records
for collection in c['Comment'].collection_names():
    if 'system' not in collection:

        # Index comments
        c['Comment'][collection].create_indexes([
            pymongo.IndexModel(
                [('_id', pymongo.TEXT)], unique=True, background=True
            ),
            pymongo.IndexModel(
                [('date', pymongo.DESCENDING)], unique=False, background=True
            )
        ])

        # Index corpora
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

# Initialize Dictionary and Counters with the top 100 most common
# ngrams. Having these at the start of the numeric index is a lookup
# efficiency concern

# Loading most common ngrams
f = resource_string(__name__, 'data/unigrams.json').decode('utf-8')
unigrams = json.loads(f)
f = resource_string(__name__, 'data/bigrams.json').decode('utf-8')
bigrams = json.loads(f)
f = resource_string(__name__, 'data/trigrams.json').decode('utf-8')
trigrams = json.loads(f)

# Inserting most common ngrams
for str_type in STR_TYPE_LIST:
    for n, stopword_list in zip([1,2,3], [unigrams, bigrams, trigrams]):
        if not c['Counter'][str_type].find_one({'n' : n}):
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

# Checking celery

@app.task
def celery_running():
    return True
assert celery_running.apply().result
