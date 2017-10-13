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

def initialize_context(quorum, topic):
    """ Initialize a streaming context given Zookeeper quorum host:port and kafka topic """
    sc = SparkContext("local[2]", appName="StreamingKafkaPCAPSizeup")
    sc.setLogLevel("WARN")

    # log4jLogger = sc._jvm.org.apache.log4j
    # l4j_logger = log4jLogger.LogManager.getLogger(__name__)

    ssc = StreamingContext(sc, 1)

    stream = KafkaUtils.createStream(ssc,
            zkQuorum=quorum,
            groupId="Spark-streaming-consumer",
            topics={topic: 1},
            valueDecoder=lambda x: Ether(base64.b64decode(x)))

    pcaps = stream.map(lambda x: x[1])
    sizes = pcaps.flatMap(packetmap)
    reduced_sizes = sizes.reduceByKey(lambda a, b: a+b)
    cumulative_sizes = sizes.reduceByKeyAndWindow(
            func=lambda a, b: a+b,
            invFunc=lambda a, b: a-b,
            windowDuration=10,
            slideDuration=1,
            filterFunc=lambda x: x[1] != 0
            )

    cumulative_sizes.pprint()

    return ssc

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: sizeup.py <zk> <topic>", file=sys.stderr)
	exit(-1)

    quorum, topic = sys.argv[1:]

    ssc = StreamingContext.getOrCreate('/tmp/sizeup',
            lambda: initialize_context(quorum, topic))

    ssc.start()
    ssc.awaitTermination()

