#!/bin/bash

cd $SPARK_HOME && \
	./bin/spark-submit \
	--jars $SPARK_HOME/external/streaming/kafka/spark-streaming-kafka-0-8-assembly_2.11-2.2.0.jar \
	--py-files ~/venvs/scapy-2.3.3/dist/scapy-2.3.3-py2.7.egg \
	sizeup.py \
	localhost:2181 \
	catchall
