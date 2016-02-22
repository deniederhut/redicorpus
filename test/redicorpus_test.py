#!/usr/bin/env python

import json
import pytest
from redicorpus import c
from redicorpus.base import redicorpus as rc
import time

def test_string():
    obj = rc.String('fried')
    assert obj.__to_tuple__() == ('fried', 'fried', 'VBN', 'String')

def test_stem():
    obj = rc.Stem('fried')
    assert obj.__to_tuple__() == ('fri', 'fried', 'VBN', 'Stem')

def test_lemma():
    obj = rc.Lemma('fried')
    assert obj.__to_tuple__() == ('fry', 'fried', 'VBN', 'Lemma')

def test_comment():
    with open('data/comment.json', 'r') as f:
        data = json.load(f)
    comment = rc.Comment(data)
    assert isinstance(comment, rc.DictLike)
    assert len(comment)
    assert comment['_id'] == 'd024gzv'
    r = comment.insert()

for collection in c['test'].collection_names():
    c['test'].drop_collection(collection)
