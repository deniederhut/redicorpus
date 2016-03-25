---
Title : redicorpus
Author : Dillon Niederhut
---

[![Build Status](https://travis-ci.org/deniederhut/redicorpus.svg?branch=master)](https://travis-ci.org/deniederhut/redicorpus)  [![codecov.io](https://codecov.io/github/deniederhut/redicorpus/coverage.svg?branch=master)](https://codecov.io/github/deniederhut/redicorpus?branch=master)

# In development -- unstable

## Description

Redicorpus builds linguistic corpora in real-time to give you temporal resultion on the order of a single day, instead of years or decades.

Its database and computing tasks are distributed in parallel, which makes it fault-tolerant and easy to scale out.

Frequently used intermediate data are computed in advance, which reduces the latency for common queries.
