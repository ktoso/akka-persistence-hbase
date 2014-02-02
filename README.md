HBase Journal for Akka Persistence
==================================

A replicated _fully asynchronous_ [Akka Persistence](http://doc.akka.io/docs/akka/snapshot/scala/persistence.html) journal backed by [Apache HBase](http://hbase.apache.org).

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

What to expect
--------------
**Is performance OK?** I did a brief and **naive** test recently (won't even call it a benchmark ;-)), but in general the plugin was able to write "from `!` to __persisted in hbase__" with around 6000 ~ 8000 messages per second.
This was tested using 3 region servers (small instances) on the Google Compute Engine, with batch writes of 200 items per batch, 50 partitions (key prefix) and 4 regions.
Recovery time for 45000 messages was around 3 seconds (keep in mind, my Actor does nothing, just receives the replayed message).

**Async:** Even though in the code it looks like it issues one `Put` at a time, this is not the case, as writes are buffered and then batch written thanks to AsyncBase.

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


[![Bitdeli Badge](https://d2weczhvl823v0.cloudfront.net/ktoso/akka-persistence-hbase/trend.png)](https://bitdeli.com/free "Bitdeli Badge")

