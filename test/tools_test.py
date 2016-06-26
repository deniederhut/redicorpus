#!/usr/bin/env python

from __future__ import absolute_import

from arrow import Arrow
from datetime import datetime, timedelta
import pytest
from redicorpus import tools

def test_parse_markdown():
    data = "[Trump's wall just got 10 feet higher!](https://youtu.be/gPfJwc8Cwao?t=19s) \n\n#Total height: 70ft. \n\n***** \n\nBot by /u/TonySesek556"
    text, links = tools.parse_markdown(data)
    assert text == "Trump's wall just got 10 feet higher! \n\n#Total height: 70ft. \n\n***** \n\nBot by /u/TonySesek556"
    assert links == ['https://youtu.be/gPfJwc8Cwao?t=19s']

def test_pos():
    assert tools.pos_to_wordnet('VB') == 'v'
    assert tools.pos_to_wordnet('RB') == 'r'
    assert tools.pos_to_wordnet('JJ') == 'a'
    assert tools.pos_to_wordnet('DILLON') == 'n'

def test_split_time():
    result = tools.split_time(
        Arrow(1, 1, 1, 1, 1).datetime, Arrow(2, 2, 2, 2, 2).datetime
    )
    assert result['n_days'] == 396
    assert result['remainder_start'] == timedelta(0, 82740)
    assert result['remainder_stop'] == timedelta(0, 7320)
    assert result['start_day'] == Arrow(1, 1, 2, 0, 0).datetime
    assert result['stop_day'] == Arrow(2, 2, 2, 0, 0).datetime
