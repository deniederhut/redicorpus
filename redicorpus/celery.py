#!/usr/bin/env python

from __future__ import absolute_import

from celery import Celery

app = Celery('redicorpus',
             broker='amqp://',
             backend='rpc://',
             include=[
                'redicorpus',
                 'redicorpus.base.redicorpus',
                 'test'
                 ]
             )

# Optional configuration, see the application user guide.
app.conf.update(
    CELERY_TASK_RESULT_EXPIRES=3600,
    CELERY_ACCEPT_CONTENT = ['pickle']
)

if __name__ == '__main__':
    app.start()
