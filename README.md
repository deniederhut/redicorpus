redicorpus
==========
the python library for real-time corpus building and querying

# Warning - this build is no longer maintained

## contents

* setup.sh - appends variables to ~/.bashrc and job to crontab
* rc_builder - script called daily by cron that pulls, parses, and stems comments, then builds them into raw ngram counts and word lists that characterize the individual day (as both chisq and tfidf values)
* rc_tracker - interactive querying functions
* comment_tracker - script to count the total number of comments returned at each hour of the day and day of the week

## structure

* rc_builder creates a directory structure like this:

~~~
redicorpus/

  corpora/
    2014_12_24/
      1gram.txt
      2gram.txt
      3gram.txt

  dailies/
    expected/
      2014_12_24.txt
    tfidf/
      2014_12_24.txt

  pages/
    2014_12_24/
      2o8fh.xml

~~~

* rc_counters creates the following directories to store results when the corresponding query is run:

~~~
redicorpus/

  context/
    vonnegut.txt

  maps/
    foma_wordmap.txt
    boko_maru_posmap.txt
    busi_busi_busi_commap.txt

  sentiment/
    blue_tunnel.csv

  trackers/
    so_it_goe.csv
~~~
