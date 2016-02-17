#!/usr/bin/env python

import json
import pytest
from redicorpus.base import redicorpus

def test_string():
    obj = redicorpus.String('try')
    assert obj.__to_tuple__() == ('try', 'try', 'NN', None)

# TODO insert tests for other StringLike
