#!/bin/env python

from nltk import download

for resource in [
                'averaged_perceptron_tagger',
                'punkt',
                'wordnet'
                ]:
    download(resource)
