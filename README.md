HBase Journal for Akka Persistence
==================================

A replicated _fully asynchronous_ [Akka Persistence](http://doc.akka.io/docs/akka/2.3-SNAPSHOT/scala/persistence.html) journal backed by [Apache HBase](http://hbase.apache.org).

Usage
-----

The artifact is published to Maven Central, so in order to use it you just have to add the following dependency:

    // build.sbt style:
    libraryDependencies += "pl.project13.scala" %% "akka-persistence-hbase" % "0.3"

Since `0.2` this library is fully **async**. Version `0.2` is compatible with Akka `2.3-M2`, the akka-persistence API has since then **changed**!

Please note that only versions `0.3+` are compatible with the latests Akka version (`2.3-SNAPSHOT`, today is: 20 Jan 2013).
This version of the API should be stable though, so go ahead and give it a spin!

Configuration
-------------

To activate the HBase journal plugin, add the following line to your Akka `application.conf`:

    akka.persistence.journal.plugin = "hbase-journal"

For more configuration options check the sources of [reference.conf](https://github.com/ktoso/akka-persistence-hbase/blob/master/src/main/resources/reference.conf).

TODO
----

* Scans can be made more parallel - due to the fact we have "partitions"
* Stress testing and some metrics

License
-------

**Apache 2.0**

Kudos
-----

Heads up to _Martin Krasser_ who helped with this project by nice links as well as his [akka-persistence-cassandra](https://github.com/krasserm/akka-persistence-cassandra) plugin and eventsourced.
