HBase plugin for Akka Persistence
==================================

A replicated _fully asynchronous_ [Akka Persistence](http://doc.akka.io/docs/akka/snapshot/scala/persistence.html) journal and snapshot store backed by [Apache HBase](http://hbase.apache.org).

<a href="https://travis-ci.org/ktoso/akka-persistence-hbase"><img src="https://travis-ci.org/ktoso/akka-persistence-hbase.png" alt="akka-persistence-hbase build status"></a> (because I'm trying to get Hbase / HDFS to become testable on Travis... not there yet).

Akka-persistance-hbase also provides a _SnapshotStore_ two implementations, one which stores directly into the messages HBase collection,
and another one that used HDFS to store larger snapshots. You must select one of the implementations in the configuration.

Usage
-----

The artifact is published to Maven Central, so in order to use it you just have to add the following dependency:

```
// build.sbt style:
libraryDependencies += "pl.project13.scala" %% "akka-persistence-hbase" % "0.3"
```

Please note that only versions `0.3+` are compatible with the latests Akka version (`2.3-SNAPSHOT`, today is: 20 Jan 2013).
This version of the API should be stable though, so go ahead and give it a spin!

Configuration
-------------

### Journal

To activate the HBase journal plugin, add the following line to your Akka `application.conf`:

```
akka.persistence.journal.plugin = "hbase-journal"
```


### SnapshotStore - HBase

Two snapshot store implementations are provided.
To activate the _HBase_ snapshot store implementation add the following line to your `application.conf`

```
# enable the hadoop-snapshot-store
akka.persistence.snapshot-store.plugin = "hadoop-snapshot-store"

# and select the snapshotter implementation
hadoop-snapshot-store {
  impl = "akka.persistence.hbase.snapshot.HBaseSnapshotter"
}
```

### SnapshotStore - HDFS
For snapshots it may be sometimes smarter to store them in HDFS directly, instead of in HBase.
This has a few advantages - it's nicer to deal with a "very big" snapshot this way, and you can easily download it by using
plain hadoop command line tooling if needed.

```
# enable the hadoop-snapshot-store
akka.persistence.snapshot-store.plugin = "hadoop-snapshot-store"

hadoop-snapshot-store {
  # and select the snapshotter implementation
  impl = "akka.persistence.hbase.snapshot.HdfsSnapshotter"

  # and set the directory where the snapshots should be saved to
  snapshot-dir = "/akka-snapshots"
}
```

### More details

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
* HDFS snapshots (~HBase snapshots are done already~)
* Stress testing and some metrics

More akka-persistence backends:
-------------------------------
A list is maintained here: https://gist.github.com/krasserm/8612920#file-akka-persistence-plugins-md

License
-------

**Apache 2.0**

Kudos
-----

Heads up to _Martin Krasser_ who helped with this project by nice links as well as his [akka-persistence-cassandra](https://github.com/krasserm/akka-persistence-cassandra) plugin and eventsourced.


[![Bitdeli Badge](https://d2weczhvl823v0.cloudfront.net/ktoso/akka-persistence-hbase/trend.png)](https://bitdeli.com/free "Bitdeli Badge")

