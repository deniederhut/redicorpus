#!/usr/bin/env python
"""
High temporal resolution, out of core, distributed corpus
building, and querying.

Built on MongoDB, Celery, and NLTK
"""

from copy import deepcopy
from datetime import datetime, timedelta
from math import log
from nltk import ngrams, word_tokenize, pos_tag, SnowballStemmer, WordNetLemmatizer
from redicorpus import c, app


# String classes

class StringLike(object):
    """Acts like a string, but contains metadata"""

    def __init__(self, data=None, pos=None):
        if data:
            if isinstance(data, str):
                self.__from_string__(data, pos)
            elif isinstance(data, tuple):
                self.__from_tuple__(data)

    def __add__(self, x):
        return str(self.cooked) + str(x)

    def __class__(self):
        return "StringLike"

    def __iter__(self):
        for character in self.cooked:
            yield character

    def __len__(self):
        return len(self.cooked)

    def __mul__(self, x):
        return self.cooked * x

    def __repr__(self):
        return "Cooked : {}, from raw : {}".format(self.cooked, self.raw)

    def __str__(self):
        return self.cooked

    def __from_string__(self, data, pos):
        self.raw = data
        if not pos:
            pos = pos_tag([data])[0][1]
        self.pos = pos
        self.str_type = self.__class__()

    def __from_tuple__(self, data):
        self.raw, self.cooked, self.pos, self.str_type = data

    def __to_tuple__(self):
        return self.cooked, self.raw, self.pos, self.str_type


class String(StringLike):
    """A string"""

    def __init__(self, data, pos=None):
        super(String, self).__init__(data, pos)
        self.cooked = self.raw

    def __class__(self):
        return "String"


class Stem(StringLike):
    """A stemmed string"""

    def __init__(self, data, pos=None):
        super(Stem, self).__init__(data, pos)
        self.cooked = self.stemmer(data)

    def __class__(self):
        return "Stem"

    def stemmer(self, data):
        return SnowballStemmer('english').stem(data)


class Lemma(StringLike):
    """A lemmatized string"""

    def __init__(self, data, pos=None):
        super(Lemma, self).__init__(data, pos)
        self.cooked = self.lemmer(data, self.pos)

    def __class__(self):
        return "Lemma"

    def lemmer(self, data, pos):
        return WordNetLemmatizer().lemmatize(data, self.pos_to_wordnet(pos))

    @staticmethod
    def pos_to_wordnet(pos):
        if pos in ['VB', 'VBD', 'VBG', 'VBN', 'VBP', 'VBZ']:
            return 'v'
        elif pos in ['RB', 'RBR', 'RBS']:
            return 'r'
        elif pos in ['JJ', 'JJR', 'JJS']:
            return 'a'
        else:
            return 'n'

# Dict classes

class DictLike(object):
    """Acts like a dict, but has mongo i/o"""

    def __init__(self, data=None, n_list=[1,2,3]):
        self.data = {}
        if data:
            self.data = data
        self.n_list = n_list
        self.str_classes = StringLike.__subclasses__()

    def __class__(self):
        return "DictLike"

    def __getitem__(self, key):
        return self.data.get(key)

    def __len__(self):
        return len(self.data.keys())

    def __repr__(self):
        return '{} from {}, with data:\n\n {}'.format(self.__class__(), self['source'], self['raw'])

    def __setitem__(self, key, value):
        self.data[key] = value

    def __str__(self):
        return str(self.data)

    def __from_dict__(self, data):
        assert isinstance(data, dict)
        assert '_id' in data
        for key in data:
            self.data[key] = data.get(key)

    def __to_dict__(self):
        return self.data

    def keys(self):
        return self.data.keys()

    def items(self):
        return self.data.items()

    def values(self):
        return self.data.values()


class Comment(DictLike):
    """A single communicative event"""

    def __init__(self, data):
        super(Comment, self).__init__()
        if isinstance(data, dict):
            self.__from_dict__(data)
        for str_type in self.str_classes:
            self[str_type.__name__] = tokenize.apply(args=[self['raw'], str_type]).result

    def __class__(self):
        return "Comment"

    def __update_body__(self, gram, n, str_type):
        collection = c[self['source']][str_type.__name__]
        raw = tuple(string_like.__to_tuple__()[0] for string_like in gram)
        term = tuple(string_like.__to_tuple__()[1] for string_like in gram)
        pos = tuple(string_like.__to_tuple__()[2] for string_like in gram)
        collection.update_one(
            {
            'date' : self['date'],
            'term' : term,
            'raw' : raw,
            'pos' : pos,
            'n' : n
            }, {
            '$inc' : {
                'count' : 1, 'total' : 1
            },
            '$addToSet' : {
                'users' : self['user'],
                'documents' : self['_id']
            },
            '$push' : {
                'polarity' : self['polarity'],
                'controversiality' : self['controversiality'],
                'emotion' : self['emotion']
            }
            },
            upsert=True)

    def __update_dictionary__(self, gram, n, str_type):
        dictionary = c[str(n) + 'gram'][str_type.__name__]
        counters = c[str(n) + 'gram']['counters']
        cooked_gram = ' '.join([item.cooked for item in gram])
        if not dictionary.find_one({'term' : cooked_gram}):
            id_counter = counters.find_one_and_update({
            'str_type' : str_type.__name__,
            },
            {
            '$inc' : {
                'counter' : 1
                }
            }, return_document=True)
            if id_counter:
                dictionary.insert_one({
                '_id' : id_counter['counter'],
                'term' : cooked_gram
                })
            else:
                counters.insert_one({
                'str_type' : str_type.__name__,
                'counter' : 0
                })
                dictionary.insert_one({
                '_id' : 0,
                'term' : cooked_gram
                })

    def __update_source__(self):
        collection = c[self['source']][type(self).__name__]
        document = deepcopy(self.data)
        for str_type in self.str_classes:
            document[str_type.__name__] = [string_like.__to_tuple__() for string_like in self[str_type.__name__]]
        collection.insert_one(document)

    def insert(self):
        self.__update_source__()
        for n in self.n_list:
            for str_type in self.str_classes:
                for gram in ngrams(self[str_type.__name__], n):
                    self.__update_dictionary__(gram, n, str_type)
                    self.__update_body__(gram, n, str_type)


# Array classes

class ArrayLike(object):
    """Acts like an array, but has database dictionary methods"""

    def __init__(self, data=None, n=1, str_type='String'):
        self.data = []
        if data:
            self.data = data
        self.n = n
        assert str_type in [subclass.__name__ for subclass in StringLike.__subclasses__()]
        self.str_type = str_type
        self.dictionary = c[str(n) + 'gram'][str_type]

    def __add__(self, other):
        if isinstance(other, int) | isinstance(other, float):
            self.data = [item + other for item in self.data]
            return self.data
        elif isinstance(other, ArrayLike):
            self.__matchlen__(other)
            result = []
            for i in range(0, len(self.data)):
                result.append(self.data[i] + other.data[i])
            return result

    def __class__(self):
        return "ArrayLike"

    def __contains__(self, key):
        if self.__getitem__(key):
            return True
        else:
            return False

    def __forcelen__(self, length):
        if length > len(self):
            self.data = self.data + [0] * (length - len(self))

    def __getitem__(self, key):
        if isinstance(key, int):
            return self.data[key]
        else:
            ix = self.__getix__(key)
            return self.data[ix]

    def __getix__(self, key):
        if isinstance(key, str):
            pass
        elif isinstance(key, StringLike):
            key = key.cooked
        elif isinstance(key, tuple) & isinstance(key[0], StringLike):
            key = ' '.join([string_like.cooked for string_like in key])
        elif isinstance(key, list) & isinstance(key[0], str):
            key = ' '.join([item for item in key])
        else:
            raise TypeError("Expected str, StringLike, or tuple of StringLikes")
        ix = self.dictionary.find_one(
        {
            'term' : key
        }, {
            '_id' : 1
            }
        )['_id']
        return ix

    def __iter__(self):
        for item in self.data:
            yield item

    def __len__(self):
        return len(self.data)

    def __matchlen__(self, other):
        if len(self) > len(other):
            other.data = other.data + [0] * (len(self) - len(other))
        elif len(other) > len(self.data):
            self.data = self.data + [0] * (len(other) - len(self.data))

    def __mul__(self, other):
        if isinstance(other, float) | isinstance(other, int):
            self.data = [item * other for item in self.data]
            return self.data
        elif isinstance(other, ArrayLike):
            self.__matchlen__(other)
            result = []
            for i in range(0, len(self.data)):
                result.append(self.data[i] * other.data[i])
            return result

    def __repr__(self):
        return '{} of length {}'.format(self.__class__(), len(self.data))

    def __setbyix__(self, key, value):
        try:
            self.data[key] = value
        except IndexError as e:
            if e.args[0] == 'list assignment index out of range':
                self.__forcelen__(key + 1)
                self.data[key] = value

    def __setitem__(self, key, value):
        if isinstance(key, int):
            self.__setbyix__(key, value)
        else:
            ix = self.__getix__(key)
            self.__setbyix__(ix, value)

    def __str__(self):
        return str(self.data)


class Vector(ArrayLike):
    """
    An ArrayLike of term frequencies for a given string type,
    count type, gram length, and time period
    """

    def __init__(self, n, str_type, count_type, source, start_date=datetime(1970,1,1), stop_date=datetime.utcnow()):
        super(Vector, self).__init__(n, str_type)
        assert source in c.database_names()
        assert str_type in [string_like.__name__ for string_like in StringLike.__subclasses__()]
        for date in [start_date, stop_date]:
            if date:
                assert isinstance(date, datetime)

        self.count_type = count_type
        self.start_date = start_date
        self.stop_date = stop_date
        self.collection = c[source][str_type]
        self.__fromdb__()

    def __fromdb__(self):
        try:
            self.__fromcache__()
        except FileNotFoundError:
            self.__fromcursor__()

    def __fromcache__(self):
        result = self.collection.find_one({
            'start_date' : self.start_date,
            'stop_date' : self.stop_date,
            self.count_type : {
                '$exists' : True
            }
        }, {
            self.count_type : 1
        })
        if result:
            self.data = result[self.count_type]
        else:
            raise FileNotFoundError

    def __fromcursor__(self):
        counts = ArrayLike(n=self.n, str_type=self.str_type)
        documents = ArrayLike(n=self.n, str_type=self.str_type)
        users = ArrayLike(n=self.n, str_type=self.str_type)
        for document in self.collection.find({
            'date' : {
                '$gt' : self.start_date, '$lt' : self.stop_date
            },
            'n' : self.n
        }):
            ix = counts.__getix__(document['term'])
            counts[ix] = document['count']
            documents[ix] = len(document['documents'])
            users[ix] = len(document['users'])
        if self.count_type == 'activation':
            self.data = list(self.activation(counts, users).data)
        elif self.count_type == 'count':
            self.data = list(counts)
        elif self.count_type == 'tf':
            self.data = list(self.tf(counts).data)
        elif self.count_type == 'tfidf':
            self.data = list(self.tfidf(counts, documents).data)
        else:
            raise ValueError("Expected one of 'activation', 'count', 'tf', or 'tfidf'")
        self.__tocache__()

    def __tocache__(self):
        self.collection.update_one({
            'start_date' : self.start_date,
            'stop_date' : self.stop_date
        }, {
            '$set' : {self.count_type : self.data}
        }, upsert=True)

    @staticmethod
    def activation(users):
        inverse_total_users = sum(users) ** -1
        return users * inverse_total_users

    @staticmethod
    def tf(counts):
        inverse_total_counts = sum(counts) ** -1
        return counts * inverse_total_counts

    @staticmethod
    def tfidf(counts, documents):
        total_documents = sum(document)
        return counts * log(total_documents/(1+documents), base=10)


class Map(ArrayLike):
    """Conditional probability map for a single term"""

    def __init__(self, term, source, data=None, position=0, time_stamp=None, time_delta=None):
        super(Map, self).__init__()
        assert isinstance(term, StringLike)
        assert source in c.database_names()

        self.term = term
        self.time_stamp = time_stamp
        self.time_delta = time_delta
        self.position = position
        self.collection = c[source]['maps']

        if data:
            self.__from_dict(data)
        if not data:
            if time_stamp:
                try:
                    self.__from_maps(term, position, time_stamp, time_delta)
                except IndexError:
                    self.__from_comments()
            else:
                self.__from_comments()

    def __from_maps(self, term, position, time_stamp, time_delta):
        try:
            self.__from_dict(self.collection.find_one({
            'term' : term,
            'position' : position,
            'time_stamp' : time_stamp,
            'time_delta' : time_delta
            }))
        except:
            raise IndexError("Document does not exist in collection")

    def __from_comments(self):
        # TODO support for ngrams
        for document in self.collection.find({
        'date' : {
            start_time : {'$gt' : self.start_time},
            stop_time : {'$lt' : self.stop_time}
            }
        }, {
        type(term) : 1
        }):
            array = Comment(document).tokenize(type(term))
            if position:
                ix = array.index(term) + position
                try:
                    self[array[ix]] += 1
                except IndexError:
                    pass
            else:
                array.remove(term)
                for gram in array:
                    self[gram] += 1

    def __from_dict(self, data):
        self.data = data[type(term)]
        self.time_stamp = data['time_stamp']
        self.time_delta = data['time_delta']
        self.position = data['position']


# Module functions

@app.task(name='redicorpus.base.redicorpus.tokenize')
def tokenize(string, str_type):
    return [str_type(token, pos) for token, pos in pos_tag(word_tokenize(string.lower()))]

def get_comment():
    """Retrieve comment from db"""
    pass
    # initialize comment object
    # return object with data

def get_body():
    """Retrieve counts by date and type"""
    pass
    # initialize body object
    # return object with data

def get_map():
    """Retrieve pre-computed map"""
    pass
    # initialize map object
    # if object in db:
    #     return object with data
    # else:
    #     build map
    #     return map

def zipf_test(x, y=None):
    """Conduct one-way or two-way Zipf test on ArrayLike objects"""
    pass
