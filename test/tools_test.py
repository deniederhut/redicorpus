#!/usr/bin/env python

from __future__ import absolute_import

import pytest
from redicorpus import tools

def test_parse_markdown():
    data = "[Trump's wall just got 10 feet higher!](https://youtu.be/gPfJwc8Cwao?t=19s) \n\n#Total height: 70ft. \n\n***** \n\nBot by /u/TonySesek556"
    text, links = tools.parse_markdown(data)
    assert text == "Trump's wall just got 10 feet higher! \n\n#Total height: 70ft. \n\n***** \n\nBot by /u/TonySesek556"
    assert links == ['https://youtu.be/gPfJwc8Cwao?t=19s']
