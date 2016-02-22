#!/usr/bin/env python
"""
High temporal resolution, out of core, distributed corpus
building, and querying.

Built on MongoDB, Celery, and NLTK
"""

from copy import deepcopy
from datetime import datetime, timedelta
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
        self.str_type = None

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
        self.cooked = self.lemmer(data)

    def __class__(self):
        return "Lemma"

    def lemmer(self, data):
        return WordNetLemmatizer().lemmatize(data, self.pos_to_wordnet(pos))

    @staticmethod
    def pos_to_wordnet(pos):
        if pos in ['VB', 'VBD', 'VBG', 'VBN', 'VBP', 'VBZ']:
            return 'v'
        elif pos in ['RB', 'RBR', 'RBS']:
            return 'r'
        elif pos in ['JJ', 'JJR', 'JJS']:
            return 'j'
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
        collection = c[str(n) + 'gram'][str_type.__name__]
        counters = c[str(n) + 'gram']['counters']
        if not collection.find({'term' : gram}):
            if counters.find({'str_type' : str_type.__name__}):
                d = counters.find_one_and_update({
                'str_type' : type(gram).__name__,
                },
                {
                '$inc' : {
                    'counter' : 1
                    }
                }, w=1)
                collection.insert({
                '_id' : d['counter'],
                'term' : gram.cooked
                })
            else:
                counters.insert_one({
                'str_type' : str_type.__name__,
                'counter' : 1
                }, w=1)
                collection.insert({
                '_id' : 0,
                'term' : gram.cooked
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
    """Acts like an array, but has mongo based dictionary methods"""

    def __init__(self, data=None):
        self.data =[]
        if data:
            self.data = data

    def __add__(self, other):
        if isinstance(other, int) | isinstance(other, float):
            self.data = [item + other for item in self.data]
            return self.data
        elif isinstance(other, ArrayLike):
            self.__forcelen__(other)
            result = []
            for i in range(0, len(self.data)):
                result.append(self.data[i] + other.data[i])
            return result

    def __class__(self):
        return "ArrayLike"

    def __contains__(self, key):
        result = self.__getitem__(key)
        if not result:
            return False
        else:
            return True

    def __forcelen__(self, other):
        if len(self) > len(other):
            other.data = other.data + [0] * (len(self) - len(other))
        elif len(other) > len(self.data):
            self.data = self.data + [0] * (len(other) - len(self.data))

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

    def __mul__(self, other):
        if isinstance(other, float) | isinstance(other, int):
            self.data = [item * other for item in self.data]
            return self.data
        elif isinstance(value, ArrayLike):
            self.__forcelen__(other)
            result = []
            for i in range(0, len(self.data)):
                result.append(self.data[i] * other.data[i])
            return result

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
            '$gt' : self.time_stamp,
            '$lt' : self.time_stamp + self.time_delta
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
