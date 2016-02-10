#!/usr/bin/env python

from __future__ import absolute_import

from celery import Celery

app = Celery('redicorpus',
             broker='mongodb://localhost:27017/celery',
             include=['redicorpus.getters.askreddit','redicorpus.builders.redicorpus'])

# Optional configuration, see the application user guide.
app.conf.update(
    CELERY_TASK_RESULT_EXPIRES=3600,
)

if __name__ == '__main__':
    app.start()
