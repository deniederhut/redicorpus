#!/usr/bin/env python

from __future__ import absolute_import

from datetime import datetime
import json
from pkg_resources import resource_string
import pytest
from redicorpus.get import reddit

@pytest.fixture
def get_data():
    string = resource_string('test', 'data/raw_reddit.json').decode('utf-8')
    return json.loads(string)

def test_parse_markdown():
    data = "[Trump's wall just got 10 feet higher!](https://youtu.be/gPfJwc8Cwao?t=19s) \n\n#Total height: 70ft. \n\n***** \n\nBot by /u/TonySesek556"
    text, links = reddit.parse_markdown(data)
    assert text == "Trump's wall just got 10 feet higher! \n\n#Total height: 70ft. \n\n***** \n\nBot by /u/TonySesek556"
    assert links == ['https://youtu.be/gPfJwc8Cwao?t=19s']

def test_response():
    data = get_data()
    response = reddit.Response(data, 'test')
    for key in response:
        assert key
    assert response['author'] == "schfourteen-teen"
    assert response['_id'] == 't1_d024gzv'
    assert response['date'] == datetime.utcfromtimestamp(1455645473.0)

def test_client():
    client = reddit.Client('test')
    assert client.user_agent
    assert client.source == 'test'
    assert client.datelimit < client.new_date
    generator = client.request()
    comment = next(generator)
    assert isinstance(comment, reddit.Response)
