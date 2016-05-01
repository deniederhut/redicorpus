#!/usr/bin/env python
"""
High temporal resolution, out of core, distributed corpus
building, and querying.

Built on MongoDB, Celery, and NLTK
"""

from __future__ import absolute_import

from arrow import Arrow, utcnow
from copy import deepcopy
from datetime import datetime, timedelta
from math import log
from nltk import ngrams, word_tokenize, pos_tag, SnowballStemmer, WordNetLemmatizer
from pymongo.errors import DuplicateKeyError
from pymongo.collection import ReturnDocument
from redicorpus import c
from redicorpus import exceptions as e
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
        return self.term + str(x)

    def __class__(self):
        return "StringLike"

    def __iter__(self):
        for character in self.term:
            yield character

    def __len__(self):
        return len(self.term)

    def __mul__(self, x):
        return self.term * x

    def __repr__(self):
        return self.term

    def __str__(self):
        return self.term

    def __fromstring__(self, data, pos):
        """
        Make instance from string data. If pos is not set, NLTK's pos algorithm is used to infer it.
        """
        self._raw = data
        self._term = None
        if not pos:
            pos = pos_tag([data])[0][1]
        self._pos = pos

    def __fromtuple__(self, data):
        """Convert self to tuple"""
        self.term, self.raw, self.pos, _ = data

    def __totuple__(self):
        """Make instance from tuple"""
        return self.term, self.raw, self.pos, self.__class__()

    @property
    def term(self):
        """Get the modified string/term/word/token"""
        return self._term

    @term.setter
    def term(self, value):
        self._term = value

    @property
    def raw(self):
        """Get the unmodified string/term/word/token"""
        return self._raw

    @raw.setter
    def raw(self, value):
        self._raw = value

    @property
    def pos(self):
        """Get the part of speech"""
        return self._pos

    @pos.setter
    def pos(self, value):
        self._pos = value


class String(StringLike):
    """A string with metadata"""

    def __init__(self, data, pos=None):
        super(String, self).__init__(data, pos)
        self.term = self.raw

    def __class__(self):
        return "String"


class Stem(StringLike):
    """A snowball stemmed string with metadata"""

    def __init__(self, data, pos=None):
        super(Stem, self).__init__(data, pos)
        if isinstance(data, str):
            self.term = self.stemmer(data)

    def __class__(self):
        return "Stem"

    def stemmer(self, data):
        """Return stemmed data"""
        return SnowballStemmer('english').stem(data)


class Lemma(StringLike):
    """A WordNet lemmatized string with metadata"""

    def __init__(self, data, pos=None):
        super(Lemma, self).__init__(data, pos)
        if isinstance(data, str):
            self.term = self.lemmer(data, self.pos)

    def __class__(self):
        return "Lemma"

    def lemmer(self, data, pos=None):
        """
        Return lemmatized data based on part of speech
        pos : str
            Should be one of 'v', 'r', 'a', or 'n'
        """
        return WordNetLemmatizer().lemmatize(data, self.pos_to_wordnet(pos))

    @staticmethod
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

class Gram(object):
    """Acts like a tuple, but for StringLike objects"""

    def __init__(self, data):
        self.gram = data
        if len(self) > 1:
            if len(set([type(item) for item in self.gram])) > 1:
                raise TypeError("Gram components must be the same type")
        self.str_type = self.gram[0].__class__()

    def __fromdb__(self, document):
        """Make instance from database representation"""
        data = []
        for ix, term in document['term']:
            data.append(tuple[
            term,
            document['raw'][ix],
            document['pos'][ix],
            document['str_type']
            ])
        self.gram = tuple([StringLike.__fromtuple__(item) for item in data])

    def __len__(self):
        return len(self.gram)

    def __str__(self):
        return ' '.join([str(term) for term in self.gram])

    def __todb__(self):
        """Convert instance into document for db compatibility"""
        return {
        'term' : tuple([item.term for item in self.gram]),
        'raw' : tuple([item.raw for item in self.gram]),
        'pos' : tuple([item.pos for item in self.gram]),
        'str_type' : self.str_type
        }

    @property
    def gram(self):
        return self._gram

    @gram.setter
    def gram(self, value):
        if isinstance(value, tuple) | isinstance(value, list):
            if isinstance(value[0], StringLike):
                self._gram = tuple(value)
            else:
                raise TypeError("Expected an iterable of StringLike objects")
        elif isinstance(value, StringLike):
            self._gram = (value,)
        else:
            raise TypeError("Expected an iterable of StringLike objects")

    @property
    def pos(self):
        """Get tuple of parts of speech"""
        return tuple([item.pos for item in self.gram])

    @property
    def raw(self):
        return tuple([item.raw for item in self.gram])

    @property
    def string_type(self):
        """Get string class of terms in gram"""
        return self._str_type

    @string_type.setter
    def string_type(self, value):
        if value in StringLike.__subclasses__().__name__:
            self._str_type = value
        else:
            raise ValueError("String type must be StringLike")

    @property
    def term(self):
        """Get tuple of terms in gram"""
        return tuple([item.term for item in self.gram])

# Dict classes

class DictLike(object):
    """A dictionary with built-in database i/o"""

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
        """Make instance from document"""
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
    """
    A single communicative event represented as a DictLike

    Has the following mandatory fields:
    _id : str
        ID of event (must be unique)
    author : str
        ID of person who created event
    children : list
        Child events
    cooked : str
        Text from event with markup formatting removed
    date : datetime.datetime
        Datetime of event creation (be careful to coerce to UTC)
    Lemma : list
        Iterable of Grams of lemmas
    links : list
        List of links pulled from markup (plaintext links not included)
    parent_id : str
        ID of parent event
    raw : str
        Text from event
    source : str
        Shortname of source of event
    Stem : list
        Iterable of Grams of stems
    String : list
        Iterable of Grams of strings
    thread_id : list
        ID of root event
    url : str
        Location of event

    Has the following optional fields:
    controversiality : int
        Site-supplied estimate of divisiveness of event
    score : int
        Site-supplied estimate of value of event
    """

    def __init__(self, data):
        super(Comment, self).__init__()
        if isinstance(data, dict):
            if 'String' in data:
                self.__fromdocument__(data)
            else:
                self.__fromdict__(data)
        for str_type in self.str_classes:
            self[str_type.__name__] = tokenize.apply(args=[self['cooked'], str_type]).result

    def __class__(self):
        return "Comment"

    def __fromdocument__(self, data):
        """Make instance from database document"""
        str_type_list = StringLike.__subclasses__()
        key_list = [subclass.__name__ for subclass in str_type_list]
        for subclass, key in zip(str_type_list, key_list):
            self.data[key] = [subclass(tuple(item)) for item in data[key]]
        for key in data:
            if key not in key_list:
                self.data[key] = data.get(key)

    def __updatebody__(self, gram):
        """Pre-calculate and cache intermediate corpus data"""
        collection = c['Body'][self['source']]
        term = gram.term
        raw = gram.raw
        pos = gram.pos
        collection.update_one(
            {
            'date' : self['date'],
            'term' : term,
            'raw' : raw,
            'pos' : pos,
            'n' : len(gram),
            'str_type' : gram.str_type
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

    def __updatedictionary__(self, gram):
        """Create dictionary entries for any new grams"""
        dictionary = c['Dictionary'][gram.str_type]
        counters = c['Counter'][gram.str_type]
        term = gram.term
        n = len(gram)
        if not dictionary.find_one({'term' : term, 'n' : n}):
            id_counter = counters.find_one_and_update({
            'n' : n,
            },
            {
            '$inc' : {
                'counter' : 1
                }
            }, return_document=ReturnDocument.AFTER)
            dictionary.insert_one({
            'ix' : id_counter['counter'],
            'term' : term,
            'n' : n
            })

    def __updatecomment__(self):
        """Insert instance into database"""
        collection = c['Comment'][self['source']]
        document = deepcopy(self.data)
        for str_type in self.str_classes:
            document[str_type.__name__] = [string_like.__totuple__() for string_like in self[str_type.__name__]]
        return collection.insert_one(document)

    def insert(self):
        """Perform all necessary database updates for instance"""
        success = False
        try:
            success = self.__updatecomment__()
        except DuplicateKeyError:
            warnings.warn("Not Implemented : id={} already in collection".format(self['_id']))
        if success:
            for n in self.n_list:
                for str_type in self.str_classes:
                    for item in ngrams(self[str_type.__name__], n):
                        gram = Gram(item)
                        self.__updatedictionary__(gram)
                        self.__updatebody__(gram)
            return success

# Array classes

class ArrayLike(object):
    """
    Acts like an array, but supports both list-like and dict-like index methods.
    """

    def __init__(self, data=None, n=1, str_type='String', null=0):
        self.data = []
        self.null = null
        if data:
            self.data = data
        self.n = n
        if str_type not in [subclass.__name__ for subclass in StringLike.__subclasses__()]:
            raise ValueError("{} is not a valid string type class".format(str_type))
        self.str_type = str_type
        self.dictionary = c['Dictionary'][self.str_type]

    def __add__(self, other):
        """
        Update values of array in place and return. Supports length coersion for ArrayLike and broadcasting for ints or floats
        """
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
        result = self.__getitem__(key)
        if result == self.null:
            return False
        else:
            return True

    def __forcelen__(self, length):
        """Increase length of array by adding self.null items"""
        if length > len(self):
            self.data = self.data + [self.null] * (length - len(self))

    def __getitem__(self, key):
        """
        Returns value at index. If key not in self, returns self.null
        key : int, str
            Either the integer index of the item, or the term that is wanted
        """
        if isinstance(key, int):
            ix = key
        else:
            ix = self.__getix__(key)
        if ix:
            try:
                return self.data[ix]
            except IndexError:
                return self.null
        else:
            return self.null

    def __getix__(self, key):
        """Get integer index given a key string"""
        if isinstance(key, str):
            pass
        elif isinstance(key, StringLike):
            key = Gram(key).term
        elif isinstance(key, Gram):
            key = key.term
        elif isinstance(key, tuple) & isinstance(key[0], str):
            key = key
        elif isinstance(key, list) & isinstance(key[0], str):
            key = tuple(key)
        else:
            raise TypeError("Expected str, StringLike, or tuple of StringLikes")
        try:
            ix = self.dictionary.find_one(
            {
                'term' : key,
                'n' : self.n
            })['ix']
        except TypeError:
            ix = None
        return ix

    def __iter__(self):
        for item in self.data:
            yield item

    def __len__(self):
        return len(self.data)

    def __matchlen__(self, other):
        """
        Match lengths of two ArrayLikes by padding the shorter one with self.null
        """
        if len(self) > len(other):
            other.data = other.data + [self.null] * (len(self) - len(other))
        elif len(other) > len(self.data):
            self.data = self.data + [self.null] * (len(other) - len(self.data))

    def __mul__(self, other):
        """
        Update values of array in place and return. Supports length coersion for ArrayLike and broadcasting for ints or floats
        """
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
        """
        Set value by index. Supports setting values beyond the length of the data.
        """
        try:
            self.data[key] = value
        except IndexError as e:
            if e.args[0] == 'list assignment index out of range':
                self.__forcelen__(key + 1)
                self.data[key] = value

    def __setitem__(self, key, value):
        """
        Set value by index or key. Supports setting values beyond the length of the data.
        """
        if isinstance(key, int):
            self.__setbyix__(key, value)
        else:
            ix = self.__getix__(key)
            if ix:
                self.__setbyix__(ix, value)
            else:
                raise ValueError("Term not in dictionary")

    def __str__(self):
        return str(self.data)


class Vector(ArrayLike):
    """
    An ArrayLike of language data for a given string type, count type, gram length, and time period.

    The following are required:
    source : str
        Short name of source of events. Must be in database.
    n : int
        Length of grams
    str_type : StringLike
        String type of event data
    count_type : str
        Type of language data. Acceptable values are 'count', 'tf', 'tfidf', and 'activation'
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
        """Try fetching vector from cache, then build from comment data"""
        try:
            self.__fromcache__()
        except e.DocumentNotFound:
            self.__fromcursor__()

    def __fromcache__(self):
        """Fetch vector from cache"""
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
            raise e.DocumentNotFound(self.n, 'date range')

    def __fromcursor__(self):
        """Build vector from comment database"""
        counts = ArrayLike(n=self.n, str_type=self.str_type)
        documents = ArrayLike(n=self.n, str_type=self.str_type, null=set())
        users = ArrayLike(n=self.n, str_type=self.str_type, null=set())
        for document in self.collection.find({
            'date' : {
                '$gt' : self.start_date, '$lt' : self.stop_date
            },
            'n' : self.n,
            'str_type' : self.str_type
        }):
            ix = counts.__getix__(document['term'])
            counts[ix] += document['count']
            documents[ix] = documents[ix] | set(document['documents'])
            users[ix] = documents[ix] | set(document['users'])
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
        """Insert vector into cache"""
        self.cache.update_one({
            'start_date' : self.start_date,
            'stop_date' : self.stop_date
        }, {
            '$set' : {self.count_type : self.data}
        }, upsert=True)

    @staticmethod
    def activation(users):
        """
        Calculate the probability that an individual used the terms in the vector
        """
        inverse_total_users = len(Vector.unique(users)) ** -1
        users = ArrayLike([Vector.length(item) for item in users])
        return users * inverse_total_users

    @staticmethod
    def tf(counts):
        """Calculate the proportional frequency of the terms in a vector"""
        inverse_total_counts = sum(counts) ** -1
        return counts * inverse_total_counts

    @staticmethod
    def tfidf(counts, documents):
        """Calculate the tf-idf of the terms in a vector"""
        tf = ArrayLike(Vector.tf(counts))
        inverse_total_documents = len(Vector.unique(documents)) ** -1
        documents = ArrayLike([Vector.length(item) for item in documents])
        idf = ArrayLike([ log( (item + 1) * inverse_total_documents ) for item in documents])
        return tf * idf

    @staticmethod
    def unique(array_like):
        """Return set of unique elements in a list of sets"""
        total_set = set()
        for sub_list in array_like.data:
            if not sub_list:
                sub_list = set()
            total_set = total_set | set(sub_list)
        return total_set

    @staticmethod
    def length(list_like):
        """Return length of a list, or 0 if empty"""
        try:
            return len(list_like)
        except TypeError:
            return 0


class Map(ArrayLike):
    """Conditional probability map for a single term"""

    def __init__(self, gram, source, position=0, start_date=Arrow(1970,1,1).datetime, stop_date=utcnow().datetime):
        super(Map, self).__init__()
        if isinstance(gram, Gram):
            self.str_type = gram.str_type
            self.term = gram.term
            self.n = len(gram)
        elif isinstance(gram, StringLike):
            gram = Gram(gram)
            self.str_type = gram.str_type
            self.term = gram.term
            self.n = len(gram)
        else:
            raise TypeError("{} must be StringLike or Gram".format(self.term))
        if source not in c['Comment'].collection_names():
            raise ValueError("{} is not a collection in Comment")

        self.start_date = start_date
        self.stop_date = stop_date
        self.position = position
        self.source = source

        self.__fromdb__()

    def __fromdb__(self):
        try:
            self.__fromcollection__()
        except e.DocumentNotFound:
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
            raise e.DocumentNotFound(self.term, 'daterange')

    def __fromcursor__(self):
        self.data = []
        for document in c['Body'][self.source].find({
            'term' : self.term,
            'date' : {'$gt' : self.start_date, '$lt' : self.stop_date},
            'str_type' : self.str_type,
            'n' : self.n
        }, {
        'documents' : 1
        }):
            for _id in document['documents']:
                comment = get_comment(_id, self.source)
                gram_list = []
                for ngram in ngrams(comment[self.str_type], self.n):
                    gram_list.append(Gram(ngram).term)
                if self.position:
                    loc = gram_list.index(self.term) + position
                    self[gram_list[loc]] + 1
                else:
                    gram_list.remove(self.term)
                    for gram in gram_list:
                        self[gram] += 1
        try:
            self * (sum(self) ** -1)
        except ZeroDivisionError:
            raise ValueError("No comments with term {} found".format(self.term))
        self.__tocollection__()

    def __tocollection__(self):
        c['Body'][self.source].insert_one({
        'term' : self.term,
        'position' : self.position,
        'start_date' : self.start_date,
        'stop_date' : self.stop_date,
        'probabilities' : self.data,
        })


# Module functions

@app.task(name='redicorpus.base.redicorpus.tokenize')
def tokenize(string, str_type):
    """Tokenize a string into StringLike objects"""
    return [str_type(token, pos) for token, pos in pos_tag(word_tokenize(string.lower()))]

def get_comment(_id, source):
    """Retrieve comment from db"""
    document = c['Comment'][source].find_one({'_id' : _id})
    if document:
        return Comment(document)
    else:
        raise e.DocumentNotFound(_id, source)

@app.task
def insert_comment(response):
    """Create comment instance and insert it"""
    return Comment(response).insert()

def get_body(source, n=1, str_type='String', count_type='count', start_date=utcnow().datetime, stop_date=utcnow().datetime):
    """Retrieve counts by date and type"""
    return Vector(source, n, str_type, count_type, start_date, stop_date)

def get_map(gram, source, n, position=0, start_date=Arrow(1970,1,1).datetime, stop_date=utcnow().datetime):
    """Retrieve pre-computed map"""
    return Map(gram, source, position, start_date, stop_date)

def get_datelimit(source):
    """Fetch last datetime events were fetched from source"""
    try:
        datelimit = c['Comment']['LastUpdated'].find_one({
            'source' : source
        })['date']
    except TypeError:
        datelimit = datetime.utcnow() - timedelta(1)
        c['Comment']['LastUpdated'].insert_one({
            'source' : source,
            'date' : datelimit
        })
    return datelimit

def set_datelimit(source, startdate):
    """Set last datetime events were fetched from source"""
    c['Comment']['LastUpdated'].replace_one({
        'source' : source
    }, {
        'source' : source,
        'date' : startdate
    })

def zipf_test(x, y=None):
    """Conduct one-way or two-way Zipf test on ArrayLike objects"""
    pass
