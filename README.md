# What it is

Practicing a collector-consumer(?) model for pcap analysis with Kafka and Scapy

# Usage

Initialize and activate a python2.7 virtual environment. Then `pip install -r requirements.txt`.

Copy the config template to `config.yaml` and fill in the blanks. Agent output can be kafka, stdout, or both. Analyst input can be only Kafka for now.

`sudo python agent.py`

and

`python analyst.py`

## Spark-specific

Set a convenient SPARK_HOME environment variable.

To take care of external dependencies:

`pip download scapy`

Untar the scapy package and cd into it, then

`python -c "import setuptools; execfile('setup.py')" bdist_egg`

Ship that egg along with the code destined for spark so the cluster can reference it.

Download the kafka streaming jars from [maven](https://repo1.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-8-assembly_2.11/2.2.0/)

And plant the jar in `$SPARK_HOME/external/streaming/kafka/`, or likewise. See the `start.sh` 1-liner for inspiration how to invoke the spark job.
