#!/bin/bash

hbase_version="0.94.10"
hbase_home_base="/opt/hbase"
hbase_home="/opt/hbase-$hbase_version"

wget "http://archive.apache.org/dist/hbase/hbase-$hbase_version/hbase-$hbase_version.tar.gz" -O /tmp/hbase.tar.gz --read-timeout=5 --tries=0
sudo mkdir -p /opt
sudo tar xf /tmp/hbase.tar.gz -C /opt
sudo chown -R `whoami`.`whoami` /opt

# configs, for pseudo dist mode (1 node)
cp -R scripts/hbase-conf/* /opt/hbase-${hbase_version}/conf/
