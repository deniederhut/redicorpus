#!/usr/bin/env python

from pymongo import MongoClient
from redicorpus.celery import app

c = MongoClient(j=True)
assert any(c.database_names())

@app.task
def celery_running():
    return True
assert celery_running.apply().result
