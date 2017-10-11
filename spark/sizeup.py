#!/bin/env python

from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from scapy.all import *

def packetmap(packet):
    """ Flatmap packet length keyed by tuple of source and destination IPs """
    if IP in packet:
        return [(packet[IP].src, packet[IP].dst), len(packet)]
    else:
        return []

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: kafka_wordcount.py <zk> <topic>", file=sys.stderr)
	exit(-1)

    sc = SparkContext("local[2]", appName="PythonStreamingKafkaPCAPSizeup")
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 1)

    quorum, topic = sys.argv[1:]

    stream = KafkaUtils.createStream(ssc, quorum, "Spark-streaming-consumer", {topic: 1}, valueDecoder=lambda x: x)
    pcaps = stream.map(lambda x: Ether(x[1]))
    sizes = pcaps.flatMap(packetmap)
    reduced_sizes = sizes.reduceByKey(lambda a, b: a+b)

    reduced_sizes.pprint()

    ssc.start()
    ssc.awaitTermination()

