# Kinesis Connector

Kinesis is Amazon’s fully managed cloud-based service for real-time processing
of large, distributed data streams.

This connector allows the use of Kinesis streams as tables in Presto, such that each data-blob (message)
in a kinesis stream is presented as a row in Presto.  A flexible table mapping approach lets us
treat fields of the messages as columns in the table.

Under the hood, a Kinesis shard iterator is used to retrieve the records, along with
a series of getRecords calls.  The shard iterator starts by default 24 hours before
the current time and works its way forward (this is configurable now, so it can
changed  as needed).  To be able to query a stream, table mappings are needed as
explained in [Table Definition] (https://github.com/stitchfix/presto-kinesis/wiki/Table-Definitions).
These table definitions can be stored on Amazon S3 (preferred) or stored in
a local directory on each Presto node.

This connector is Read-Only connector. It can only fetch data from
kinesis streams, but can not create streams or push data into the al
ready existing streams.


# Steps to use

* Create a ``kinesis.properties`` file in ``$PRESTO_HOME/etc/catalog`` directory. See [Connector Configuration] (https://github.com/stitchfix/presto-kinesis/wiki/Connector-Configuration)
* Create a json table definition file for every presto-kinesis table. See [Table Definition] (https://github.com/stitchfix/presto-kinesis/wiki/Table-Definitions).  These can be added to S3 or a local directory (by default ``$PRESTO_HOME/etc/kinesis``).


This distribution contains a sample kinesis.properties file that can be used as a starting point (in etc/catalog).
Comments above each property summarize what each property is for.
