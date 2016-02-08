#!/usr/bin/env python
"""
High resolution, distributed corpus building, querying, and testing

Built on MongoDB and Celery
"""

from collections import defaultdict
from nltk import pos_tag, SnowballStemmer, WordNetLemmatizer
from pymongo import MongoClient

c = MongoClient()

class StringLike(object):
    """Acts like a string, but contains metadata"""

    def __init__(self, data, pos=None):
        assert isinstance(data, str)
        if not pos:
            pos = pos_tag([data])
        self.raw = data
        self.pos = pos
        self.language = 'english'

    def __add__(self, x):
        return str(self.cooked) + str(x)

    def __class__(self):
        return "stringLike"

    def __len__(self):
        return len(self.cooked)

    def __repr__(self):
        return "Cooked : {}, from raw : {}".format(self.cooked, self.raw)

    def set_language(self, language):
        assert language in ['english', 'spanish']
        self.language = language

    def __str__(self):
        return self.cooked

class DictLike(object):
    """Acts like a dict, but has mongo i/o"""

    def __init__(self, data=None):
        if not data:
            data = {}
        assert isinstance(data, dict)
        self.data = data

    def __repr__(self):
        return '{} from {}, with data {}'.format(self.data['_id'], self.data['source'], self.data['raw'])

    def __str__(self):
        return str(self.data)


class ArrayLike(object):
    """Acts like an array, but has mongo based dict methods"""

    def __init__(self):
        pass


class Stem(StringLike):
    """A stemmed string"""

    def __init__(self, data, pos=None):
        super(Stem, self).__init__(data, pos)
        self.cooked = self.stemmer(data)

    def stemmer(self, data):
        return SnowballStemmer(self.language).stem(data)


class Lemma(StringLike):
    """A lemmatized string"""

    def __init__(self, data, pos=None):
        super(Lemma, self).__init__(data, pos)
        self.cooked = self.lemmatize(data)

    def lemmatize(self, data):
        return WordNetLemmatizer().lemmatize(data)


class Comment(dictLike):
    """A single communicative event and metadata"""

    def __init__(self):
        pass

    @app.task
    def to_db(self):
        insert document
        if successful:
            add document to counting queue

    @app.task
    def count(self, _id):
        create instance of comment from queue
        get date
        for token type:
            for gram length:
                for ngram:
                    update document by ngram and date
                    update total by date


class body(arrayLike):
    """All of the frequency counts for a day"""

    def __init__(self):
        pass

    def __sum__():
        pass

    def __index__():
        pass

    def __repr__():
        pass

    def from_db(self):
        set instance data equal to db query result

    def get_activation(self):
        pass

    def get_tfidf(self):
        pass

class map(arrayLike):
    """Conditional probability map for terms"""

    def __init__(self):

    def test():

    def control():

def get_comment():
    """Retrieve comment from db"""
    initialize comment object
    return object with data

def get_body():
    """Retrieve counts by date and type"""
    initialize body object
    return object with data

def get_map():
    """Retrieve pre-computed map"""
    initialize map object
    if object in db:
        return object with data
    else:
        build map
        return map
