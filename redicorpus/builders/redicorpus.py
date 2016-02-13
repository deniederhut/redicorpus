#!/usr/bin/env python
"""
High temporal resolution, out of core, distributed corpus
building, and querying.

Built on MongoDB, Celery, and NLTK
"""

from datetime import datetime, timedelta
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
        self.data = {}
        if data:
            self.data = data

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

    def __from_dict(self, data):
        assert isinstance(data, dict)
        assert '_id' in data
        for key in self.data:
            self.data[key] = data.get(key)

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
    # TODO insert unique user names
    # TODO insert document counts
    def update_gram(collection, gram, date):
        collection.update_one(
            {'_id' : gram, 'date' : date},
            {'$inc' :
                {'count' : 1}
            }
        )


class ArrayLike(object):
    """Acts like an array, but has mongo based dictionary methods"""

    def __init__(self, data=None):
        self.data =[]
        if data:
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

    def __contains__(self, key):
        result = self.__getitem__(key)
        if not result:
            return False
        else:
            return True

    def __getitem__(self, key):
        if isinstance(key, int):
            return self.data[key]
        else:
            string_type = type(String('a'))
            if isinstance(key, StringLike):
                key = key.cooked
                string_type = type(key)
            i = c['dictionary'][string_type].find_one(
            {
                'term' : key
            }, {
                'index' : 1
                }
            )['index']
            return self.data[i]

    def __iter__(self):
        for item in self.data:
            yield item

    def __len__(self):
        return len(self.data)

    def __mul__(self, value):
        return [int(item) * float(value) for item in self.data]

    def __repr__(self):
        return 'ArrayLike of length {}'.format(len(self.data))

    def __setitem__(self, key, value):
        if isinstance(key, int):
            self.data[key] = value
        else:
            string_type = type(String('a'))
            if isinstance(key, StringLike):
                key = key.cooked
                string_type = type(key)
            i = c['dictionary'][string_type].find_one(
            {
                'term' : key
            }, {
                'index' : 1
                }
            )['index']
            self.data[i] = value

    def __str__(self):
        return str(self.data)


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
        self.cooked = self.lemmer(data)

    def lemmer(self, data):
        return WordNetLemmatizer().lemmatize(data)


# Class declarations - DictLike

class Comment(DictLike):
    """A single communicative event"""

    def __init__(self, data=None):
        super(Comment, self).__init__()
        self.data = {
            '_id' : None,
            'permalink' : None,
            'source' : None,
            'timestamp' : None,
            'thread_id' : None,
            'parent_id' : None,
            'child_ids' : [],
            'hrefs' : [],
            'author' : None,
            'polarity' : None,
            'text' : None,
        }
        if data:
            self.data = self.__from_dict(data)


# Class declarations - ArrayLike

class Body(ArrayLike):
    """All of the frequency counts for a time period"""

    def __init__(self, source, string_type, gram_length, time_stamp, time_delta=timedelta(1)):
        assert source in c.database_names()
        assert string_type in ('string', 'stem', 'lemma')
        assert isinstance(time_stamp, datetime)
        assert isinstance(time_delta, timedelta)

        collection = c[source][string_type]
        if time_delta.seconds: #if resolution < day
            # result = self.__catch_body()
            raise BaseException('Resolution not supported')
        else:
            result = self.__from_db(collection, gram_length, time_stamp, time_delta)
        self.data = result['count']
        self.documents = result['documents']
        self.users = result['users']

    def __catch_body(self, source, string_type, time_stamp, time_delta):
        # initialize result
        for document in c[source]['comments'].find({
            'datetime' : {
                '$gt' : time_stamp, '$lt' : time_stamp + time_delta
        }}):
            comment = Comment(document)
            # count comment
        # return result

    def __from_db(self, collection, time_stamp, time_delta):
        result = {
            'count' : ArrayLike(),
            'documents' : ArrayLike(),
            'users' : ArrayLike()
        }
        for document in collection.find({
            '_id' :
                {'$gt' : time_stamp, '$lt' : time_stamp + time_delta
        }}):
            result['count'] += ArrayLike(document['count'])
            result['document'] += ArrayLike(document['document'])
            result['users'] += ArrayLike(document['users'])
        return result

    def activation(self):
        total_users = sum(self.users) ** -1
        return [count * total_users for count in self.users]

    def count(self):
        return self.data

    def tfidf(self):
        result = []
        for count, document in zip (self.count(), self.tfidf):
            try:
                result.append(count / document)
            except ZeroDivisionError:
                result.append(0)
        return result


class Map(ArrayLike):
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
