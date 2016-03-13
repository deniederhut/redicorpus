#!/usr/bin/env python

from datetime import datetime
import json
import pytest
from redicorpus import c
from redicorpus.base import redicorpus as rc
import time

count_type_list = ['activation', 'count', 'tf', 'tfidf']
gram_length_list = [1, 2, 3]
str_type_list = ['String', 'Stem', 'Lemma']

def test_string():
    obj = rc.String('fried')
    assert str(obj) == 'fried'
    assert len(obj) == 5
    assert obj + ' pickles' == 'fried pickles'
    assert obj * 2 == 'friedfried'
    new_obj = []
    for item in obj:
        new_obj.append(item)
    assert new_obj == ['f', 'r', 'i', 'e', 'd']
    assert obj.__totuple__() == ('fried', 'fried', 'VBN', 'String')

def test_stem():
    obj = rc.Stem('fried')
    assert obj.__totuple__() == ('fri', 'fried', 'VBN', 'Stem')

def test_lemma():
    obj = rc.Lemma('fried')
    assert obj.__totuple__() == ('fry', 'fried', 'VBN', 'Lemma')

def test_dict_like():
    with pytest.raises(TypeError):
        rc.DictLike().__fromdict__('blue')
    with pytest.raises(ValueError):
        rc.DictLike().__fromdict__({'raw' : 'blue'})
    obj = rc.DictLike()
    obj['test'] = 42
    assert dict(obj)
    assert list(obj.items()) == [('test', 42)]

def test_comment():
    with open('test/data/comment.json', 'r') as f:
        data = json.load(f)
    data['date'] = datetime.fromtimestamp(data['date'])
    comment = rc.Comment(data)
    assert isinstance(comment, rc.DictLike)
    assert len(comment)
    assert comment['_id'] == 'd024gzv'
    assert dict(comment)
    assert str(comment)
    assert comment.keys()
    assert comment.values()
    comment.insert()
    comment.insert()
    document = c['Comment']['test'].find_one()
    assert isinstance(rc.Comment(document)['Lemma'][0], rc.Lemma)

def test_update_body():
    for str_type in str_type_list:
        document = c['Body']['test'].find_one()
        assert document
        assert len(document['users']) == 1
        assert document['count'] == len(document['polarity'])

def test_update_dictionary():
    for str_type in str_type_list:
        for gram_length in gram_length_list:
            counter = c['Counter'][str_type].find_one({'n' : gram_length})
            assert counter
            assert counter['counter'] > 1
            document = c['Dictionary'][str_type].find_one()
            assert document
            assert document['ix'] <= counter['counter']

def test_update_source():
    document = c['Comment']['test'].find_one({'_id' : "d024gzv"})
    assert document

def test_get_comment():
    comment = rc.get_comment('test','d024gzv')
    assert comment

def test_array_like():
    with pytest.raises(ValueError):
        rc.ArrayLike(n=1, str_type='Frayed')
    array = rc.ArrayLike([1,2,3,4], 1, 'String')
    assert str(array)
    assert array.n == 1
    assert array + 1 == [2, 3, 4, 5]
    assert array + rc.ArrayLike([0, 1], 1, 'String') == [2, 4, 4, 5]
    assert array * 2 == [4, 6, 8, 10]
    assert array * rc.ArrayLike([0, 1], 1, 'String') == [0, 6, 0, 0]
    assert 2 in array
    assert 'python' not in array
    assert array['the']
    array['the'] = 1
    assert array[rc.String('the')]

def test_vector():
    with pytest.raises(ValueError):
        rc.Vector(n=1, str_type='String', count_type='Lemma', source='Blue')
    with pytest.raises(ValueError):
        rc.Vector(n=1, str_type='Frayed', count_type='Lemma', source='test')
    with pytest.raises(TypeError):
        rc.Vector(n=1, str_type='String', count_type='Lemma', source='test', start_date='now')
    for count_type in count_type_list:
        for gram_length in gram_length_list:
            for str_type in str_type_list:
                vector = rc.Vector(n=gram_length, str_type=str_type, count_type=count_type, source='test')
                assert vector.n == gram_length
                assert vector.start_date == datetime(1970, 1, 1, 0, 0)
                assert len(vector) >= 100

def test_get_body():
    vector = rc.get_body(source='test', start_date=datetime(2016,2,16), stop_date=datetime(2016,2,17))
    assert vector

def test_map():
    mapping = rc.Map('term'='proof', source='test')

def test_get_map():
    rc.get_map()

def test_zipf_test():
    rc.zipf_test(rc.ArrayLike())

def test_cleanup():
    for database in c.database_names():
        if database not in ['local', 'admin', 'test']:
            c.drop_database(database)
