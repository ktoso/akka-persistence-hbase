#! /bin/bash

# Sets up the build on a travis 12.04 ubuntu

# Die on error
set -e

date

# Get some hardware details
set -x
hostname
free -m
df -h
set +x

sudo apt-get update --force-yes -y

# Make ntp install script deal with large hostnames
#
# See
#   https://bugs.launchpad.net/u`tu/+source/ntp/+bug/971314/comments/16
#   https://bugs.launchpad.net/ubuntu/+source/liblockfile/+bug/941968
sudo mv /usr/bin/lockfile-create /usr/bin/lockfile-create.bak
sudo apt-get install --force-yes -y ntp
sudo mv /usr/bin/lockfile-create.bak /usr/bin/lockfile-create

# Need this for add-apt-repository
sudo apt-get install --force-yes -y python-software-properties

# Add cloudera repos for CHD4
release="precise" # $(lsb_release -s -c) # precise is the only one actually supported
cloudera_repo_url="http://archive.cloudera.com/cdh4/ubuntu/$release/amd64/cdh"

sudo tee /etc/apt/sources.list.d/cloudera.list > /dev/null <<EOF
deb [arch=amd64] $cloudera_repo_url $release-cdh4 contrib
deb-src $cloudera_repo_url $release-cdh4 contrib
EOF

curl -s "$cloudera_repo_url/archive.key" | sudo apt-key add -

# Get up to date with the new repo
sudo apt-get update --force-yes -y
