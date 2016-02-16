#!/usr/bin/env python

from pymongo import MongoClient

c = MongoClient(j=True)
assert any(c.database_names())
