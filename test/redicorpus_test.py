#!/usr/bin/env python

import json
import pytest
from redicorpus.base import redicorpus as rc
import time

def test_string():
    obj = rc.String('try')
    assert obj.__to_tuple__() == ('try', 'try', 'NN', None)

# TODO insert tests for other StringLike

def test_comment():
    with open('data/comment.json', 'r') as f:
        data = json.load(f)
    comment = rc.Comment(data)
    assert isinstance(comment, rc.DictLike)
    assert len(comment)
    assert comment['_id'] == 'd024gzv'
    r = comment.insert()
