#!/usr/bin/env python

import pymongo
from redicorpus.celery import app

str_collections = ['String', 'Stem', 'Lemma']
source_collections = ['Askreddit']

# Initializing Mongo Databases

c = pymongo.MongoClient()

for collection in str_collections:

    # Initialize counter
    try:
        c['Counter'].create_collection(collection,w=2)
    except pymongo.errors.CollectionInvalid:
        pass
    c['Counter'][collection].create_indexes([
        pymongo.IndexModel(
            [('n', pymongo.ASCENDING)], unique=True, background=False
        )
    ])

    # Initialize dictionary
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

for collection in source_collections:

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
