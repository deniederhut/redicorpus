#!/usr/bin/env python
"""
High temporal resolution, out of core, distributed corpus
building, and querying.

Built on MongoDB, Celery, and NLTK
"""

from arrow import Arrow, utcnow
from copy import deepcopy
from datetime import datetime
from math import log
from nltk import ngrams, word_tokenize, pos_tag, SnowballStemmer, WordNetLemmatizer
from pymongo.errors import DuplicateKeyError
from redicorpus import c
from redicorpus.celery import app
import warnings


# String classes

class StringLike(object):
    """Acts like a string, but contains metadata"""

    def __init__(self, data=None, pos=None):
        if data:
            if isinstance(data, str):
                self.__fromstring__(data, pos)
            elif isinstance(data, tuple):
                self.__fromtuple__(data)

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

    def __fromstring__(self, data, pos):
        self.raw = data
        if not pos:
            pos = pos_tag([data])[0][1]
        self.pos = pos
        self.str_type = self.__class__()

    def __fromtuple__(self, data):
        self.cooked, self.raw, self.pos, self.str_type = data

    def __totuple__(self):
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
        if isinstance(data, str):
            self.cooked = self.stemmer(data)

    def __class__(self):
        return "Stem"

    def stemmer(self, data):
        return SnowballStemmer('english').stem(data)


class Lemma(StringLike):
    """A lemmatized string"""

    def __init__(self, data, pos=None):
        super(Lemma, self).__init__(data, pos)
        if isinstance(data, str):
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

    def __fromdict__(self, data):
        if not isinstance(data, dict):
            raise TypeError("Expected dict, not {}".format(type(data)))
        if '_id' not in data:
            raise ValueError("Comments must have an '_id' field")
        for key in data:
            self.data[key] = data.get(key)

    def __todict__(self):
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
            if 'String' in data:
                self.__fromdocument__(data)
            else:
                self.__fromdict__(data)
        for str_type in self.str_classes:
            self[str_type.__name__] = tokenize.apply(args=[self['raw'], str_type]).result

    def __class__(self):
        return "Comment"

    def __fromdocument__(self, data):
        str_type_list = StringLike.__subclasses__()
        key_list = [subclass.__name__ for subclass in str_type_list]
        for subclass, key in zip(str_type_list, key_list):
            self.data[key] = [subclass(tuple(item)) for item in data[key]]
        for key in data:
            if key not in key_list:
                self.data[key] = data.get(key)


    def __updatebody__(self, gram, n, str_type):
        collection = c['Body'][self['source']]
        term = tuple(string_like.__totuple__()[0] for string_like in gram)
        raw = tuple(string_like.__totuple__()[1] for string_like in gram)
        pos = tuple(string_like.__totuple__()[2] for string_like in gram)
        collection.update_one(
            {
            'date' : self['date'],
            'term' : term,
            'raw' : raw,
            'pos' : pos,
            'n' : n,
            'str_type' : str_type.__name__
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

    def __updatedictionary__(self, gram, n, str_type):
        dictionary = c['Dictionary'][str_type.__name__]
        counters = c['Counter'][str_type.__name__]
        cooked_gram = ' '.join([item.cooked for item in gram])
        if not dictionary.find_one({'term' : cooked_gram, 'n' : n}):
            id_counter = counters.find_one_and_update({
            'n' : n,
            },
            {
            '$inc' : {
                'counter' : 1
                }
            }, return_document=True)
            dictionary.insert_one({
            'ix' : id_counter['counter'],
            'term' : cooked_gram,
            'n' : n
            })

    def __updatecomment__(self):
        collection = c['Comment'][self['source']]
        document = deepcopy(self.data)
        for str_type in self.str_classes:
            document[str_type.__name__] = [string_like.__totuple__() for string_like in self[str_type.__name__]]
        return collection.insert_one(document)

    def insert(self):
        success = False
        try:
            success = self.__updatecomment__()
        except DuplicateKeyError:
            warnings.warn("Not Implemented : id={} already in collection".format(self['_id']))
        if success:
            for n in self.n_list:
                for str_type in self.str_classes:
                    for gram in ngrams(self[str_type.__name__], n):
                        self.__updatedictionary__(gram, n, str_type)
                        self.__updatebody__(gram, n, str_type)

# Array classes

class ArrayLike(object):
    """Acts like an array, but has database dictionary methods"""

    def __init__(self, data=None, n=1, str_type='String'):
        self.data = []
        if data:
            self.data = data
        self.n = n
        if str_type not in [subclass.__name__ for subclass in StringLike.__subclasses__()]:
            raise ValueError("{} is not a valid string type class".format(str_type))
        self.str_type = str_type
        self.dictionary = c['Dictionary'][self.str_type]

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
        try:
            self.__getitem__(key)
            return True
        except TypeError:
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
            'term' : key,
            'n' : self.n
        })['ix']
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

    def __init__(self, source, n, str_type, count_type, start_date=Arrow(1970,1,1).datetime, stop_date=utcnow().datetime):
        super(Vector, self).__init__(n=n, str_type=str_type)
        if source not in c['Comment'].collection_names():
            raise ValueError("{} is not a collection in the Comment database".format(source))
        if str_type not in [string_like.__name__ for string_like in StringLike.__subclasses__()]:
            raise ValueError("{} is not a valid string type class".format(str_type))
        for date in [start_date, stop_date]:
            if not isinstance(date, datetime):
                raise TypeError("{} is not a datetime.datetime object".format(date))

        self.count_type = count_type
        self.start_date = start_date
        self.stop_date = stop_date
        self.collection = c['Body'][source]
        self.cache = c['BodyCache'][source]
        self.__fromdb__()

    def __fromdb__(self):
        try:
            self.__fromcache__()
        except FileNotFoundError:
            self.__fromcursor__()

    def __fromcache__(self):
        result = self.cache.find_one({
            'n' : self.n,
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
            'n' : self.n,
            'str_type' : self.str_type
        }):
            ix = counts.__getix__(document['term'])
            counts[ix] = document['count']
            documents[ix] = set(document['documents'])
            users[ix] = set(document['users'])
        if self.count_type == 'activation':
            self.data = self.activation(users)
        elif self.count_type == 'count':
            self.data = counts.data
        elif self.count_type == 'tf':
            self.data = self.tf(counts)
        elif self.count_type == 'tfidf':
            self.data = self.tfidf(counts, documents)
        else:
            raise ValueError("Expected one of 'activation', 'count', 'tf', or 'tfidf'")
        self.__tocache__()

    def __tocache__(self):
        self.cache.update_one({
            'start_date' : self.start_date,
            'stop_date' : self.stop_date
        }, {
            '$set' : {self.count_type : self.data}
        }, upsert=True)

    @staticmethod
    def activation(users):
        inverse_total_users = len(Vector.unique(users)) ** -1
        users = ArrayLike([Vector.length(item) for item in users])
        return users * inverse_total_users

    @staticmethod
    def tf(counts):
        inverse_total_counts = sum(counts) ** -1
        return counts * inverse_total_counts

    @staticmethod
    def tfidf(counts, documents):
        tf = ArrayLike(Vector.tf(counts))
        inverse_total_documents = len(Vector.unique(documents)) ** -1
        documents = ArrayLike([Vector.length(item) for item in documents])
        idf = ArrayLike([ log( (item + 1) * inverse_total_documents ) for item in documents])
        return tf * idf

    @staticmethod
    def unique(array_like):
        total_set = set()
        for sub_list in array_like.data:
            if not sub_list:
                sub_list = set()
            total_set = total_set | set(sub_list)
        return total_set

    @staticmethod
    def length(list_like):
        try:
            return len(list_like)
        except TypeError:
            return 0


class Map(ArrayLike):
    """Conditional probability map for a single term"""

    def __init__(self, term, source, n, position=0, start_date=Arrow(1970,1,1).datetime, stop_date=utcnow().datetime):
        super(Map, self).__init__(n=n)
        if isinstance(term, tuple):
            if isinstance(term[0], StringLike):
                self.str_type = term[0].__class__()
                self.term = ' '.join(term)
            else:
                raise TypeError("{} must be a string like class".format(term))
        else:
            if isinstance(term, StringLike):
                self.str_type = term.__class__()
            else:
                raise TypeError("{} must be a string like or tuple".format(term))
        if source not in c['Comment'].collection_names():
            raise ValueError("{} is not a collection in Comment")

        self.term = term
        self.start_date = start_date
        self.stop_date = stop_date
        self.position = position
        self.n = n
        self.source = source

    def __fromdb__(self):
        try:
            self.__fromcollection__()
        except FileNotFoundError:
            self.__fromcursor__()

    def __fromcollection__(self):
        try:
            self.data = c['Map'][self.source].find_one({
                'term' : self.term,
                'position' : self.position,
                'start_date' : self.start_date,
                'stop_date' : self.stop_date
            })['probabilities']
        except TypeError:
            raise FileNotFoundError

    def __fromcursor__(self):
        self.data = []
        for document in c['Body'][self.source].find({
            'term' : self.term,
            'date' : {'$gt' : self.start_time, '$lt' : self.stop_time},
            'str_type' : self.str_type,
            'n' : self.n
        }, {
        'documents' : 1
        }):
            for _id in document['documents']:
                comment = get_comment(_id, self.source)
                ngram_list = comment[str_type]
                if self.position:
                    loc = ngram_list.index(term) + position
                    self[' '.join(ngram_list[loc])] + 1
                else:
                    ngram_list = comment[str_type].remove(term)
                    for ngram in ngram_list:
                        self[' '.join(ngram)] + 1
        self * (sum(self) ** -1)
        self.__tocollection__()

        def __tocollection__(self):
            c['Body'][source].update_one({
            'term' : self.term,
            'position' : self.position,
            'start_date' : self.start_date,
            'stop_date' : self.stop_date,
            'probabilities' : self.data,
            })


# Module functions

@app.task(name='redicorpus.base.redicorpus.tokenize')
def tokenize(string, str_type):
    return [str_type(token, pos) for token, pos in pos_tag(word_tokenize(string.lower()))]

def get_comment(_id, source):
    """Retrieve comment from db"""
    document = c['Comment'][source].find_one({'_id' : _id})
    if document:
        return Comment(document)
    else:
        raise FileNotFoundError("No comment with id {} from source {}".format(_id, source))

def get_body(source, n=1, str_type='String', count_type='count', start_date=utcnow().datetime, stop_date=utcnow().datetime):
    """Retrieve counts by date and type"""
    return Vector(source, n, str_type, count_type, start_date, stop_date)

def get_map(term, source, n, position=0, start_date=Arrow(1970,1,1).datetime, stop_date=utcnow().datetime):
    """Retrieve pre-computed map"""
    return Map(term, source, n, position, start_date, stop_date)

def zipf_test(x, y=None):
    """Conduct one-way or two-way Zipf test on ArrayLike objects"""
    pass
