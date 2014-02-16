#! /bin/bash

# Sets up the build on a travis 12.04 ubuntu

# Die on error
set -e

date

sudo apt-get install --force-yes -y hadoop-0.20-conf-pseudo hadoop-hdfs-datanode \
  hadoop-hdfs-namenode hadoop-hdfs-secondarynamenode hbase-master hbase-regionserver \
  liblzo2-dev libsnappy-dev oracle-java7-installer zookeeper-server

# We don't need MR at all, but hadoop-0.20-conf-pseudo brings it in.  Turn it off.
sudo service hadoop-0.20-mapreduce-jobtracker stop || true
sudo service hadoop-0.20-mapreduce-tasktracker stop || true
sudo update-rc.d hadoop-0.20-mapreduce-jobtracker disable
sudo update-rc.d hadoop-0.20-mapreduce-tasktracker disable

# HDFS

# Create the state directory
sudo mkdir -p /var/lib/hadoop-hdfs/cache/hdfs/dfs/{name,data,namesecondary}
sudo chown -R hdfs:hdfs /var/lib/hadoop-hdfs/cache/hdfs/dfs/{name,data,namesecondary}
sudo chmod -R 700 /var/lib/hadoop-hdfs/cache/hdfs/dfs/{name,data,namesecondary}

# Format the hadoop filesystem.
sudo -u hdfs hdfs namenode -format -nonInteractive

sudo service hadoop-hdfs-datanode restart
sudo service hadoop-hdfs-namenode restart
sudo service hadoop-hdfs-secondarynamenode restart

# Zoo keeper
sudo -u zookeeper zookeeper-server-initialize || true
sudo service zookeeper-server restart

# Hbase

sudo -u hdfs hadoop fs -mkdir -p /hbase
sudo -u hdfs hadoop fs -chown hbase /hbase

# Tell HBase to not start its own zookeeper.  Tweak the memory & gc settings as well.
sudo sed -i                                                                           \
  -e 's/# export HBASE_MANAGES_ZK=true/export HBASE_MANAGES_ZK=false/'                \
  -e 's/export HBASE_OPTS="-XX:+UseConcMarkSweepGC"/'\
'export HBASE_OPTS="-XX:MaxPermSize=512M -XX:+UseParNewGC -XX:+UseConcMarkSweepGC"/'  \
  /etc/hbase/conf/hbase-env.sh

sudo tee /etc/hbase/conf/hbase-site.xml > /dev/null <<'XML'
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>

  <property>
    <name>hbase.zookeeper.quorum</name>
    <value>localhost</value>
  </property>

  <property>
    <name>hbase.rootdir</name>
    <value>hdfs://localhost:8020/hbase</value>
  </property>

  <property>
    <name>hbase.cluster.distributed</name>
    <value>true</value>
  </property>

  <property>
    <name>dfs.support.append</name>
    <value>true</value>
  </property>

  <property>
    <name>dfs.client.read.shortcircuit</name>
    <value>true</value>
  </property>

</configuration>
XML

sudo service hbase-master restart
sudo service hbase-regionserver restart

