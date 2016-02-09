#!/usr/bin/env python
"""
High temporal resolution, distributed corpus building, querying,
and testing

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

    def __iter__(self):
        for character in self.data:
            yield character

    def __len__(self):
        return len(self.cooked)

    def __mul__(self, x):
        return self.cooked * x

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

    def __add__(self, x):
        pass

    def __class__(self):
        return "DictLike"

    def __getitem(self, key):
        return self.data[key]

    def __len__(self):
        return len(self.data.keys())

    def __repr__(self):
        return '{} from {}, with data {}'.format(self.data['_id'], self.data['source'], self.data['raw'])

    def __setitem__(self, key, value):
        self.data[key] = value

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
    """Acts like an array, but has mongo based dictionary methods"""

    def __init__(self, data=None):
        if not data:
            self.data = []
        else:
            assert isinstance(data, list)
            self.data = data

    def __add__(self, other):
        result = []
        if len(self.data) > len(other):
            other = other + [0] * (len(self.data) - len(other))
        elif len(other) > len(self.data):
            self.data = self.data + [0] * (len(other) - len(self.data))
        for i in range(0, len(self.data)):
            result[i] = self.data[i] + other[i]
        return result

    def __class__(self):
        return "ArrayLike"

    def __contains__(self, term):
        result = self.__getitem__(term)
        if not result:
            return False
        else:
            return True

    def __getitem__(self, x):
        if isinstance(x, int):
            return self.data[x]
        else:
            string_type = type(String('a'))
            if isinstance(x, StringLike):
                x = x.cooked
                string_type = type(x)
            i = c['dictionary'][string_type].find_one(
            {
                'term' : x
            }, {
                'index' : 1
                }
            )['index']
            return self.data[i]

    def __iter__(self):
        for item in self.data:
            yield item

    def __mul__(self, value):
        return [int(item) * float(value) for item in self.data]

    def __repr__(self):
        return 'ArrayLike of length {}'.format(len(self.data))

    def __setitem__(self, x, value):
        if isinstance(x, int):
            self.data[x] = value
        else:
            string_type = type(String('a'))
            if isinstance(x, StringLike):
                x = x.cooked
                string_type = type(x)
            i = c['dictionary'][string_type].find_one(
            {
                'term' : x
            }, {
                'index' : 1
                }
            )['index']
            self.data[i] = value


# Class declarations - StringLike

class String(StringLike):
    """A string"""

    def __init__(self, data, pos=None):
        super(String, self).__init__(data, pos)
        self.cooked = None


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
