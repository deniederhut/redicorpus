#!/usr/bin/env python

from __future__ import absolute_import

from datetime import datetime, timedelta
from pkg_resources import require
import praw
import re
from redicorpus import c
from redicorpus.base import redicorpus

class Client(object):

    def __init__(self, source):
        self.user_agent = "redicorpus version {} by /u/MonsieurDufayel".format(require('redicorpus')[0].version)
        self.c = praw.Reddit(user_agent=self.user_agent)
        self.datelimit = get_datelimit(source)
        self.new_date = datetime.utcnow()
        self.source = source

    def getlisting(self, params=None):
        return list(self.c.get_comments(self.source, **kwargs))

    def request(self):
        r = self.getlisting()
        date = datetime.utcfromtimestamp(r[0].created_utc)
        while date > self.datelimit:
            for comment in r:
                yield Response(comment, self.source).translate()
            after = r[-1].name
            r = self.getlisting(params={'after' : after})
            date = datetime.utcfromtimestamp(r[0].created_utc)

class Response(object):

    def __init__(self, response, source):
        self.translation = {
        '_id' : response.id,
        'url' : response.permalink,
        'thread_id' : response.link_id,
        'parent_id' : response.parent_id,
        'children' : response.replies,
        'raw' : response.body,
        'links' : [],
        'source' : source,
        'date' : datetime.fromtimestamp(response.created_utc),
        'author' : response.author.name,
        'controversiality' : response.controversiality,
        'score' : response.score
        }

    def translate(self):
        raw = self.translation['raw']
        cooked = deepcopy(raw)
        p = re.compile(r'\[(?P<text>.+)\]\((?P=<link>.+)\)')
        for match in p.findall(cooked):
            self.translation['links'].append(match.group('link'))
            cooked = cooked.replace(match.group(), match.group('text'))
        self.translation['cooked'] = cooked
        return self.translation

def get_datelimit(source):
    try:
        datelimit = c['Comment']['LastUpdated'].find_one({
            'source' : source
        })['date']
    except TypeError:
        datelimit = datetime.utcnow() - timedelta(1)
    return datelimit

def set_datelimit(source, startdate):
    c['Comment']['LastUpdated'].update_one({
        'source' : source
    }, {
        'date' : startdate
    })

def run(source):
    reddit = Client(source=source)
    for comment in reddit.request():
        redicorpus.Comment(comment).insert()
    set_datelimit(reddit.new_date)

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('source', help='Subreddit name from which to draw comments \nFor all subreddits, use "all"')
    args = parser.parse_args()

    run(source=args.source)
