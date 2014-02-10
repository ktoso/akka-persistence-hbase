#!/bin/bash

echo "/etc/hosts:"
cat /etc/hosts
echo ""

hadoop_version="1.2.1"

wget --no-check-certificate http://raw.github.com/fs111/grrrr/master/grrr -O /tmp/grrr && chmod +x /tmp/grrr
/tmp/grrr /hadoop/common/hadoop-${hadoop_version}/hadoop-${hadoop_version}.tar.gz -O /tmp/hadoop.tar.gz --read-timeout=5 --tries=0
sudo mkdir -p /opt
sudo tar xf /tmp/hadoop.tar.gz -C /opt
sudo chown -R `whoami`.`whoami` /opt

# configs, for pseudo dist mode (1 node)
cp -R scripts/hadoop-conf/* /opt/hadoop-${hadoop_version}/conf/


# fix ssh access, starting hbase does ssh to localhost
mv ~/.ssh/id_rsa ~/.ssh/id_rsa.bak 2> /dev/null
mv ~/.ssh/id_rsa.pub ~/.ssh/id_rsa.pub.bak 2> /dev/null

if [ ! -f ~/.ssh/id_rsa ]; then
  echo "Echo, no ~/.ssh/id_rsa, generating new one..."
  ssh-keygen -t rsa -C your_email@youremail.com -P '' -f ~/.ssh/id_rsa
  cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys

  echo "Host localhost
    StrictHostKeyChecking no
    BatchMode yes" >> ~/.ssh/config

  cat ~/.ssh/config
  chmod g-rw,o-rw ~/.ssh/*

fi


