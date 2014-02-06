#!/bin/bash

wget http://apache.mirrors.lucidnetworks.net/hbase/hbase-0.96.0/hbase-0.96.0-hadoop2-bin.tar.gz
tar xzf hbase-0.96.0-hadoop2-bin.tar.gz
cd hbase-0.96.0-hadoop2

# fix ssh access, starting hbase does ssh to localhost
mv ~/.ssh/id_rsa ~/.ssh/id_rsa.bak
mv ~/.ssh/id_rsa.pub ~/.ssh/id_rsa.pub.bak
ssh-keygen -t rsa -C your_email@youremail.com -P '' -f ~/.ssh/id_rsa
cat /home/travis/.ssh/id_rsa.pub >> /home/travis/.ssh/authorized_keys
ln -s /home/travis/.ssh/authorized_keys /home/travis/.ssh/authorized_keys2

echo "Host localhost
   StrictHostKeyChecking no
      BatchMode yes" >> ~/.ssh/config

cat ~/.ssh/config
chmod g-rw,o-rw /home/travis/.ssh/*

cd ..

