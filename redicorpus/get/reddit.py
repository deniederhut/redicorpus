#!/usr/bin/env python

from __future__ import absolute_import

from datetime import datetime, timedelta
from pkg_resources import require
import praw
from redicorpus import objects, tools

class Client(object):

    def __init__(self, source):
        self.user_agent = "redicorpus version {} by /u/MonsieurDufayel".format(require('redicorpus')[0].version)
        self.c = praw.Reddit(user_agent=self.user_agent)
        self.datelimit = objects.get_datelimit(source)
        self.new_date = datetime.utcnow()
        self.source = source

    def getlisting(self, params=None):
        return self.c.get_comments(self.source, **params)

    def request(self):
        params = {'limit' : None, 'sort' : 'new'}
        r = self.getlisting(params)
        for comment in r:
            if datetime.utcfromtimestamp(comment.created_utc) > self.datelimit:
                if isinstance(comment, praw.objects.Comment):
                    result = Response(comment, self.source)
                    if result['author']:
                        yield result
            else:
                break
        objects.set_datelimit(self.source, self.new_date)

class Response(object):

    def __init__(self, response, source):
        if isinstance(response, praw.objects.Comment):
            response = vars(response)
        self.response = response
        self.translation = {
        'source' : source,
        'get_module' : 'reddit'
        }
        self.__translate__()

    def __iter__(self):
        for key in self.translation.keys():
            yield key

    def __getitem__(self, key):
        return self.translation.get(key)

    def __setitem__(self, key, value):
        self.translation[key] = value

    def __translate__(self):
        self['_id'] = self.response.get('name')
        self['url'] = self.response.get('permalink')
        self['thread_id'] = self.response.get('link_id')
        self['parent_id'] = self.response.get('parent_id')
        self['raw'] = self.response.get('body')
        self['date'] = datetime.utcfromtimestamp(self.response['created_utc'])
        try:
            self['author'] = self.response.get('author').name
        except:
            self['author'] = None
        self['controversiality'] = self.response.get('controversiality')
        self['score'] = self.response.get('score')
        try:
            self['children'] = self.response['replies']
        except KeyError:
            self['children'] = []
        self['cooked'], self['links'] = tools.parse_markdown(self['raw'])


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('source', help='Subreddit name from which to draw comments \nFor all subreddits, use "all"')
    args = parser.parse_args()

    reddit = Client(source=args.source)
    for comment in reddit.request():
        objects.insert_comment.apply_async(args=[comment.translation])
