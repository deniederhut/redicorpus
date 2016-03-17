#!/bin/env python

import pytest
from redicorpus import c
from redicorpus.ask import trackers

def test_track_counts():
    trackers.track_counts()

def test_track_activation():
    trackers.track_activation()

def test_track_emotion():
    trackers.track_emotion()

def test_track_polarity():
    trackers.track_polarity()

def test_top_n_grams():
    trackers.top_n_grams()
