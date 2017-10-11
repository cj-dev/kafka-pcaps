#!/bin/env python

from __future__ import print_function

import sys
import logging
import traceback
import base64

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from scapy.all import IP, Ether

def packetmap(packet):
    """ Flatmap packet length keyed by tuple of source and destination IPs """
    if IP in packet:
        return [((packet[IP].src, packet[IP].dst), len(packet))]
    else:
        return []

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: kafka_wordcount.py <zk> <topic>", file=sys.stderr)
	exit(-1)

    sc = SparkContext("local[2]", appName="PythonStreamingKafkaPCAPSizeup")
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 1)

    # log4jLogger = sc._jvm.org.apache.log4j
    # l4j_logger = log4jLogger.LogManager.getLogger(__name__)

    quorum, topic = sys.argv[1:]

    stream = KafkaUtils.createStream(ssc,
            zkQuorum=quorum,
            groupId="Spark-streaming-consumer",
            topics={topic: 1},
            valueDecoder=lambda x: Ether(base64.b64decode(x)))

    pcaps = stream.map(lambda x: x[1])
    sizes = pcaps.flatMap(packetmap)
    reduced_sizes = sizes.reduceByKey(lambda a, b: a+b)

    reduced_sizes.pprint()

    ssc.start()
    ssc.awaitTermination()

