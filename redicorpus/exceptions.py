#!/bin/env python

class DocumentNotFound(BaseException):

    def __init__(self, _id, db):

        self.message = "Document '{}' does not exist in {}".format(_id, db)
        super(DocumentNotFound, self).__init__(self.message)
