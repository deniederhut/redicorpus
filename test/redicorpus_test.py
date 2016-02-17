#!/usr/bin/env python

import json
import pytest
from redicorpus.base import redicorpus as rc

def test_string():
    obj = rc.String('try')
    assert obj.__to_tuple__() == ('try', 'try', 'NN', None)

# TODO insert tests for other StringLike

def test_comment():
    with open('data/comment.json', 'r') as f:
        data = json.load(f)
    obj = rc.Comment(data)
    assert len(obj) == 11
    assert obj['_id'] == 'd024gzv'
