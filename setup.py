#!/usr/bin/env python

from distutils.core import setup

setup(
    name='redicorpus',
    version='0.1.0',
    description='Real-time corpus building and querying',
    long_description='',
    url='https://github.com/deniederhut/redicorpus',
    author='Dillon Niederhut',
    author_email='dillon.niederhut@gmail.com',
    license="BSD 2-Clause",
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Science/Research',
        'Topic :: Text Processing :: Linguistic',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
    keywords='linguistics semantics diffusion timeseries',
    packages=['redicorpus', 'redicorpus.api', 'redicorpus.get'],
    package_data={'redicorpus' : ['data/*']},
    requires=[
            'datetime',
            'celery',
            'glob',
            'json',
            'logging',
            'nltk',
            'os',
            'pymongo',
            're',
            'requests',
            'time',
            'yaml'],
    extras_require={
        'test': ['pytest'],
    },
)
