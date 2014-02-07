#!/bin/bash

BUILD_DIR=`pwd`

wget --no-check-certificate http://raw.github.com/fs111/grrrr/master/grrr -O /tmp/grrr && chmod +x /tmp/grrr
/tmp/grrr /hadoop/common/hadoop-1.2.1/hadoop-1.2.1.tar.gz -O /tmp/hadoop.tar.gz --read-timeout=5 --tries=0
sodo mkdir -p /opt
sudo tar xf /tmp/hadoop.tar.gz -C /opt

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

