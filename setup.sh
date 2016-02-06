#!/bin/bash

# Adding PPAs
## for Mongo
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv EA312927
echo "deb http://repo.mongodb.org/apt/ubuntu trusty/mongodb-org/3.2 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-3.2.list

# Install from apt-get
sudo apt-get update
sudo apt-get install -y mongodb byobu openssl python

# Install from pip
sudo pip install -U celery

# Install from downloads
## rabbit-mq
wget https://www.rabbitmq.com/releases/rabbitmq-server/v3.6.0/rabbitmq-server_3.6.0-1_all.deb
sudo dpkg rabbitmq-server_3.6.0-1_all.deb

# Moving config files
## celery
sudo cp etc/celery /etc/conf.d/celery

# Starting daemons
sudo invoke-rc.d rabbitmq-server start
sudo service mongod start
sudo systemctl start celery.service
