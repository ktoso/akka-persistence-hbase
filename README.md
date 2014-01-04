HBase Journal for Akka Persistence
==================================

A replicated [Akka Persistence](http://doc.akka.io/docs/akka/2.3-M2/scala/persistence.html) journal backed by [Apache HBase](http://hbase.apache.org).

Usage
-----

The artifact is published to Maven Central, so in order to use it you just have to add the following dependency:

    // build.sbt style:
    libraryDependencies += "pl.project13.scala" %% "akka-persistence-hbase" % "0.1-SNAPSHOT"

Configuration
-------------

To activate the Cassandra journal plugin, add the following line to your Akka `application.conf`:

    akka.persistence.journal.plugin = "hbase-journal"

Please keep in mind that the HBase APIs are not async, thus this journal also _is NOT async by default_.
You can however use a simple wrapper that wraps all ops in a Future...
To do so just add the bellow to your config:

    hbase-journal.class = "akka.persistence.journal.hbase.HBaseAsyncWriteJournal"

    # whereas the default is:
    hbase-journal.class = "akka.persistence.journal.hbase.HBaseSyncWriteJournal"

The async journal will be implemented soon by using a fully async driver (the default hbase one's are not).

TODO
----

* Rewrite the Async Journal to use: [AsyncBase](https://github.com/OpenTSDB/asynchbase).

License
-------

**Apache 2.0**

Kudos
-----

This work is based on the work done by _Martin Krasser_ on his [akka-persistence-cassandra](https://github.com/krasserm/akka-persistence-cassandra) plugin.