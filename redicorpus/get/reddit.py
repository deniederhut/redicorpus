#!/usr/bin/env python

from __future__ import absolute_import

from datetime import datetime, timedelta
from pkg_resources import require
import praw
import re
from redicorpus.base import redicorpus

class Client(object):

    def __init__(self, source):
        self.user_agent = "redicorpus version {} by /u/MonsieurDufayel".format(require('redicorpus')[0].version)
        self.c = praw.Reddit(user_agent=self.user_agent)
        self.datelimit = redicorpus.get_datelimit(source)
        self.new_date = datetime.utcnow()
        self.source = source

    def getlisting(self, params=None):
        return list(self.c.get_comments(self.source, params))

    def request(self):
        r = self.getlisting()
        date = datetime.utcfromtimestamp(r[0].created_utc)
        while date > self.datelimit:
            for comment in r:
                yield Response(comment, self.source)
            after = r[-1].name
            r = self.getlisting(params={'after' : after})
            date = datetime.utcfromtimestamp(r[0].created_utc)
        redicorpus.set_datelimit(self.new_date)

class Response(object):

    def __init__(self, response, source):
        if isinstance(response, praw.objects.Comment):
            response = vars(response)
        self.response = response
        self.translation = {
        'source' : source,
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
        self['author'] = self.response.get('author')
        self['controversiality'] = self.response.get('controversiality')
        self['score'] = self.response.get('score')
        try:
            self['children'] = self.response['replies']
        except KeyError:
            self['children'] = []
        self['cooked'], self['links'] = parse_markdown(self['raw'])


def parse_markdown(text):
    link_list = []
    p = re.compile(r'\[(?P<text>.+)\]\((?P<link>.+)\)')
    for match in p.finditer(text):
        link_list.append(match.group('link'))
        text = text.replace(match.group(), match.group('text'))
    return text, link_list


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('source', help='Subreddit name from which to draw comments \nFor all subreddits, use "all"')
    args = parser.parse_args()

    reddit = Client(source=args.source)
    for comment in reddit.request():
        redicorpus.insert_comment.delay(comment)
