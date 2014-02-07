#!/bin/sh

/opt/hadoop-1.2.1/bin/hadoop namenode -format <<HERE
Y
HERE
/opt/hadoop-1.2.1/bin/start-all.sh
