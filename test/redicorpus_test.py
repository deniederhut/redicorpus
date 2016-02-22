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

def test_dict_like():
    obj = rc.DictLike()
    obj['test'] = 42
    assert list(obj.items()) == [('test', 42)]

def test_comment():
    with open('test/data/comment.json', 'r') as f:
        data = json.load(f)
    comment = rc.Comment(data)
    assert isinstance(comment, rc.DictLike)
    assert len(comment)
    assert comment['_id'] == 'd024gzv'
    r = comment.insert()

def test_update_body():
    for str_type in rc.StringLike.__subclasses__():
        document = c['test'][str_type.__name__].find_one()
        assert document
        assert len(document['users']) == 1
        assert document['count'] == len(document['polarity'])

def test_update_dictionary():
    counter = c['1gram']['counters'].find_one({'str_type' : 'Lemma'})
    assert counter
    assert counter['counter'] > 1
    document = c['1gram']['Lemma']
    assert document
    assert document['_id'] < counter['counter']

def test_update_source():
    document = c['test']['Comment'].find_one({'_id' : "d024gzv"})
    assert document

def test_array_like():
    pass

def test_body():
    pass

def test_map():
    pass

def test_cleanup():
    for collection in c['test'].collection_names():
        c['test'].drop_collection(collection)
