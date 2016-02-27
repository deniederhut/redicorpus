#!/usr/bin/env python

import pymongo
from redicorpus.celery import app

ngram_collections = ['1gram', '2gram', '3gram']
str_databases = ['String', 'Stem', 'Lemma']

# Initializing Mongo Databases

c = pymongo.MongoClient()

for collection in ngram_collections:
    try:
        c['Counter'].create_collection(collection,w=2)
    except pymongo.errors.CollectionInvalid:
        pass
    c['Counter'][collection].create_indexes([
        pymongo.IndexModel(
            [('str_type', pymongo.TEXT)], unique=True, background=False
        )
    ])

for database in str_databases:
    for collection_name in ngram_collections:
        try:
            c[database].create_collection(collection, w=2)
        except pymongo.errors.CollectionInvalid:
            pass
        c[database][collection].create_indexes([
            pymongo.IndexModel(
                [('_id', pymongo.ASCENDING)], unique=True, background=False
            ),
            pymongo.IndexModel(
                [('term', pymongo.TEXT)], unique=True, background=False
            )
        ])


# Checking celery

@app.task
def celery_running():
    return True
assert celery_running.apply().result
