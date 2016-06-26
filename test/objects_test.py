#!/usr/bin/env python

from arrow import Arrow
from datetime import datetime
import json
from pkg_resources import resource_string
import pytest
from redicorpus import c, objects
import time

gram_length_list = [1, 2, 3]

def test_string():
    obj = objects.String('fried')
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
    obj = objects.Stem('fried')
    assert obj.__totuple__() == ('fri', 'fried', 'VBN', 'Stem')

def test_lemma():
    obj = objects.Lemma('fried')
    assert obj.__totuple__() == ('fry', 'fried', 'VBN', 'Lemma')

def test_dict_like():
    with pytest.raises(TypeError):
        objects.DictLike().__fromdict__('blue')
    with pytest.raises(ValueError):
        objects.DictLike().__fromdict__({'raw' : 'blue'})
    obj = objects.DictLike()
    obj['test'] = 42
    assert dict(obj)
    assert list(obj.items()) == [('test', 42)]

def test_comment():
    with open('test/data/comment.json', 'r') as f:
        data = json.load(f)
    data['date'] = datetime.utcfromtimestamp(data['date'])
    comment = objects.Comment(data)
    assert isinstance(comment, objects.DictLike)
    assert len(comment)
    assert comment['_id'] == 'd024gzv'
    assert dict(comment)
    assert str(comment)
    assert comment.keys()
    assert comment.values()
    comment.insert()
    comment.insert()
    document = c['Comment']['test'].find_one()
    assert isinstance(objects.Comment(document)['Lemma'][0], objects.Lemma)

def test_update_body():
    for str_type in objects.StringLike.__subclasses__():
        document = c['Body']['test'].find_one({'str_type' : str_type.__name__})
        assert document
        assert len(document['users']) == 1
        assert document['count'] == len(document['polarity'])

def test_update_dictionary():
    for str_type in objects.StringLike.__subclasses__():
        for gram_length in gram_length_list:
            counter = c['Counter'][str_type.__name__].find_one({'n' : gram_length})
            assert counter
            assert counter['counter'] > 1
            document = c['Dictionary'][str_type.__name__].find_one()
            assert document
            assert document['ix'] <= counter['counter']

def test_update_source():
    document = c['Comment']['test'].find_one({'_id' : "d024gzv"})
    assert document

def test_get_comment():
    comment = objects.get_comment('d024gzv', 'test')
    assert comment

def test_insert_comment():
    c['Comment']['test'].delete_one({'_id' : 'd024gzv'})
    data = json.loads(resource_string('test', 'data/comment.json').decode('utf-8'))
    obj = objects.insert_comment.delay(data)
    assert obj.get()

def test_array_like():
    with pytest.raises(ValueError):
        objects.ArrayLike(n=1, str_type='Frayed')
    array = objects.ArrayLike([1,2,3,4], 1, objects.String)
    assert str(array)
    assert array.n == 1
    assert array + 1 == [2, 3, 4, 5]
    assert array + objects.ArrayLike([0, 1], 1, objects.String) == [2, 4, 4, 5]
    assert array * 2 == [4, 6, 8, 10]
    assert array * objects.ArrayLike([0, 1], 1, objects.String) == [0, 6, 0, 0]
    assert 2 in array
    assert (objects.String('python'), ) not in array
    assert array[objects.String('the')]
    array['the'] = 1
    assert array[objects.String('the')]

def test_vector():
    with pytest.raises(ValueError):
        objects.Vector(n=1, str_type=objects.String, count_type=objects.Activation, source='Blue')
    with pytest.raises(ValueError):
        objects.Vector(n=1, str_type='Frayed', count_type=objects.Tf, source='test')
    with pytest.raises(TypeError):
        objects.Vector(n=1, str_type=objects.String, count_type='Lemma', source='test', start_date='now')
    for count_type in objects.Count.__subclasses__():
        for gram_length in gram_length_list:
            for str_type in objects.StringLike.__subclasses__():
                vector = objects.Vector(n=gram_length, str_type=str_type, count_type=count_type, source='test')
                assert vector.n == gram_length
                assert vector.start_date == Arrow(1970, 1, 1, 0, 0).datetime
                assert len(vector) >= 100

def test_get_body():
    vector = objects.get_body(source='test', start_date=Arrow(2016,2,15).datetime, stop_date=Arrow(2016,2,18).datetime)
    assert vector

def test_map():
    mapping = objects.Map(gram=objects.String('proof'), source='test')

def test_get_map():
    objects.get_map(gram=objects.String('proof'), source='test', n=1)

def test_get_datelimit():
    assert objects.get_datelimit('test') < datetime.utcnow()

def test_set_datelimit():
    date = datetime(2015,1,1)
    objects.set_datelimit('test', date)
    assert objects.get_datelimit('test') == date
