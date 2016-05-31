#!/bin/env python

import re

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
