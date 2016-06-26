#!/bin/env python

from arrow import Arrow
import re
from datetime import datetime, timedelta

def parse_markdown(text):
    link_list = []
    p = re.compile(r'\[(?P<text>.+)\]\((?P<link>.+)\)')
    for match in p.finditer(text):
        link_list.append(match.group('link'))
        text = text.replace(match.group(), match.group('text'))
    return text, link_list

def pos_to_wordnet(pos):
    """
    Convert NLTK-style part of speech tag to WordNet-style part of speech tag
    """
    if pos in ['VB', 'VBD', 'VBG', 'VBN', 'VBP', 'VBZ']:
        return 'v'
    elif pos in ['RB', 'RBR', 'RBS']:
        return 'r'
    elif pos in ['JJ', 'JJR', 'JJS']:
        return 'a'
    else:
        return 'n'

def split_time(start_date, stop_date):
    """
    Split start and stop datetime objects into a period of whole days, and the remainder on either end, and return it as a dictionary
    """
    result = {}
    result['start_day'] = Arrow(
        start_date.year, start_date.month, start_date.day
    ).datetime + timedelta(1)
    result['remainder_start'] = result['start_day'] - start_date
    result['stop_day'] = Arrow(
        stop_date.year, stop_date.month, stop_date.day
    ).datetime
    result['remainder_stop'] = stop_date - result['stop_day']
    result['n_days'] = (result['stop_day'] - result['start_day']).days
    return result
