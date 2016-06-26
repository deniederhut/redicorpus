#!/usr/bin/env python

from __future__ import absolute_import

from celery import Celery

app = Celery('redicorpus',
             broker='amqp://',
             backend='db+sqlite:///tmp_results.sqlite',
             include=[
                'redicorpus',
                'redicorpus.objects',
                'test'
                 ]
             )

# Optional configuration, see the application user guide.
app.conf.update(
    CELERY_TASK_RESULT_EXPIRES = 3600,
    CELERY_MAX_CACHED_RESULTS = 1000,
    CELERY_ACCEPT_CONTENT = ['pickle']
)

if __name__ == '__main__':
    app.start()
