#!/usr/bin/env python
"""
High resolution, distributed corpus building, querying, and testing

Built on MongoDB and Celery
"""

from collections import defaultdict
from nltk import ngrams, word_tokenize, pos_tag, SnowballStemmer, WordNetLemmatizer
from pymongo import MongoClient

c = MongoClient()

# Metaclass declarations

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
        return "StringLike"

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

    def count(self):
        date = self.data['date']
        for gram_type in (String, Stem, Lemma):
            data = [gram_type(token) for token in word_tokenize(self.data)]
            for gram_length in (1,2,3):
                for gram in ngrams(data, gram_length):
                    collection = c[self.data['source']][gram_type]
                    self.update_gram.apply_async(collection, gram, date)
                    self.update_total.apply_async(collection, gram, date)

    @staticmethod
    @app.task
    # TODO insert user names
    # TODO insert comment _ids
    def update_gram(collection, gram, date):
        collection.update_one(
            {'_id' : gram, 'date' : date,
            {'$inc' :
                {'count' : 1}
            }
        )

    @staticmethod
    @app.task
    def update_total(collection, gram, date):
        collection.update_one(
            {'_id' : 'TOTAL', 'date' : date},
            {'$inc' :
                {'count' : 1}
            }
        )


class ArrayLike(object):
    """Acts like an array, but has mongo based dict methods"""

    def __init__(self):
        pass


# Class declarations - StringLike

class String(StringLike):
    """A string"""

    def __init__(self, data, pos=None):
        super(Stem, self).__init__(data, pos)


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


# Class declarations - DictLike

class Comment(DictLike):
    """A single communicative event and metadata"""

    def __init__(self, data=None):
        super(Comment, self).__init__(data)

    @app.task
    def to_db(self):
        c[self.data['source']]['comment'].insert_one(self.data)


# Class declarations - ArrayLike

class body(ArrayLike):
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


class map(ArrayLike):
    """Conditional probability map for terms"""

    def __init__(self):

    def test():

    def control():


# Module functions

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
