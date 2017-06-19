from __future__ import print_function

import sys
import json

from pyspark import SparkContext

"""
Create test data in HBase first:
hbase(main):016:0> create 'test', 'f1'
0 row(s) in 1.0430 seconds
hbase(main):017:0> put 'test', 'row1', 'f1:a', 'value1'
0 row(s) in 0.0130 seconds
hbase(main):018:0> put 'test', 'row1', 'f1:b', 'value2'
0 row(s) in 0.0030 seconds
hbase(main):019:0> put 'test', 'row2', 'f1', 'value3'
0 row(s) in 0.0050 seconds
hbase(main):020:0> put 'test', 'row3', 'f1', 'value4'
0 row(s) in 0.0110 seconds
hbase(main):021:0> scan 'test'
ROW                           COLUMN+CELL
 row1                         column=f1:a, timestamp=1401883411986, value=value1
 row1                         column=f1:b, timestamp=1401883415212, value=value2
 row2                         column=f1:, timestamp=1401883417858, value=value3
 row3                         column=f1:, timestamp=1401883420805, value=value4
4 row(s) in 0.0240 seconds
"""
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("""
        Usage: hbase_inputformat <host> <table>
        Run with example jar:
        ./bin/spark-submit --driver-class-path /path/to/example/jar \
        /path/to/examples/hbase_inputformat.py <host> <table> [<znode>]
        Assumes you have some data in HBase already, running on <host>, in <table>
          optionally, you can specify parent znode for your hbase cluster - <znode>
        """, file=sys.stderr)
        exit(-1)

    host = sys.argv[1]
    table = sys.argv[2]
    sc = SparkContext(appName="HBaseInputFormat")

    # Other options for configuring scan behavior are available. More information available at
    # https://github.com/apache/hbase/blob/master/hbase-server/src/main/java/org/apache/hadoop/hbase/mapreduce/TableInputFormat.java
    conf = {"hbase.zookeeper.quorum": host, "hbase.mapreduce.inputtable": table}
    if len(sys.argv) > 3:
        conf = {"hbase.zookeeper.quorum": host, "zookeeper.znode.parent": sys.argv[3],
                "hbase.mapreduce.inputtable": table}
    keyConv = "org.apache.spark.examples.pythonconverters.ImmutableBytesWritableToStringConverter"
    valueConv = "org.apache.spark.examples.pythonconverters.HBaseResultToStringConverter"

    hbase_rdd = sc.newAPIHadoopRDD(
        "org.apache.hadoop.hbase.mapreduce.TableInputFormat",
        "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
        "org.apache.hadoop.hbase.client.Result",
        keyConverter=keyConv,
        valueConverter=valueConv,
        conf=conf)
    hbase_rdd = hbase_rdd.flatMapValues(lambda v: v.split("\n")).mapValues(json.loads)

    output = hbase_rdd.collect()
    for (k, v) in output:
        print((k, v))

    sc.stop()
