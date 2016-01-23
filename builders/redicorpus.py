#!/usr/bin/env python
"""
High temporal resultion linguistic corpus
Built on MongoDB
"""

initialize db client as c

class stringlike(object):
    """Acts like a string, but contains preprocessed string data"""


class dictLike(object):
    """Acts like a dict, but has mongo i/o"""


class arrayLike(object):
    """Acts like an array, but has mongo based dict methods"""


class stem(stringLike):
    """A stemmed string"""

    def __init__(self):
        pass



class lemma(stringLike):
    """A lemmatized string"""

    def __init__(self):
        pass



class comment(dictLike):
    """A single communicative event and metadata"""

    def __init__(self):
        pass

    def __repr__(self):
        pass

    def from_getter(self, data):
        pass

    def from_db():
        set instance data equal to db query result

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
