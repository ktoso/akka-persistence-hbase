HBase plugin for Akka Persistence
==================================

A replicated _asynchronous_ [Akka Persistence](http://doc.akka.io/docs/akka/snapshot/scala/persistence.html) journal and snapshot store backed by [Apache HBase](http://hbase.apache.org).

<a href="https://travis-ci.org/ktoso/akka-persistence-hbase"><img src="https://travis-ci.org/ktoso/akka-persistence-hbase.png" alt="akka-persistence-hbase build status"></a>

Akka-persistance-hbase also provides a _SnapshotStore_ two implementations, one which stores directly into the messages HBase collection,
and another one that used HDFS to store larger snapshots. You must select one of the implementations in the configuration.

Usage
-----

The artifact is published to Maven Central, so in order to use it you just have to add the following dependency:

```
// build.sbt style:
libraryDependencies += "pl.project13.scala" %% "akka-persistence-hbase" % "0.4.0"
```

Compatibility grid:

| HBase plugin   | Akka Persistence    | 
| -------------- |:-------------------:| 
| `0.4.1`        | `2.3.4+`            |

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

# optional configuration:
hadoop-snapshot-store {
  hbase {
    # Name of the table to be used by the journal
    table = "akka_snapshots"

    # Name of the family to be used by the journal
    family = "snapshot"
  }
}
```

### SnapshotStore - HDFS

**Will be deprecated in favour of it's own plugin. Please search for `akka-persistence-hdfs`.**

For snapshots it may be sometimes smarter to store them in HDFS directly, instead of in HBase.
This has a few advantages - it's nicer to deal with a "very big" snapshot this way, and you can easily download it by using
plain hadoop command line tooling if needed.

```
# enable the hadoop-snapshot-store
akka.persistence.snapshot-store.plugin = "hadoop-snapshot-store"

hadoop-snapshot-store {
  mode = hdfs

  # and set the directory where the snapshots should be saved to
  snapshot-dir = "/akka-snapshots"
}
```

### More details

For more configuration options check the sources of [reference.conf](https://github.com/ktoso/akka-persistence-hbase/blob/master/src/main/resources/reference.conf).

Perf characteristics
--------------------
I did a brief and **naive** test recently (won't even call it a benchmark ;-)), but in general the plugin was able to write "from `!` to __persisted in hbase__" with around 6000 ~ 8000 messages per second.
This was tested using 3 region servers (small instances) on the Google Compute Engine, with batch writes of 200 items per batch, 50 partitions (key prefix) and 4 regions.
Recovery time for 45000 messages was around 3 seconds (keep in mind, my Actor does nothing, just receives the replayed message).

**Hotspot avoidance** HBase users will probably know that writing sequential keys is pretty bad for HBase as it forces one region server to do all the work (take all the writes), while the rest of the cluster sitts there doing nothing -- that's pretty bad for scaling out. Similarily to http://blog.sematext.com/2012/04/09/hbasewd-avoid-regionserver-hotspotting-despite-writing-records-with-sequential-keys/ this plugin avoids hotspotting by seeding each write with a partition number, so the key is in format `partition-persistenceId-seqNr`. Thanks to this multiple servers take part in writes - and you can scale out better. For reads this obviously is a bit worse - but thanks to running scans on each partition in *parallel* (and then resequencing them so theythe events arrive in the expected order) the performance is actually quite reasonable.

**Async:** Even though in the code it looks like it issues one `Put` at a time, this is not the case, as writes are buffered and then batch written thanks to AsyncBase.

More akka-persistence backends:
-------------------------------
A full list of community driven persistence plugins is maintained here: [akka persistence plugins @ akka.io/community](http://akka.io/community/#plugins-to-akka-persistence)

License
-------

**Apache 2.0**

Kudos
-----

Heads up to _Martin Krasser_ who helped with this project by nice links as well as his [akka-persistence-cassandra](https://github.com/krasserm/akka-persistence-cassandra) plugin and eventsourced.
