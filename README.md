# What it is

Practicing a collector-consumer(?) model for pcap analysis with Kafka and Scapy

# Usage

Initialize and activate a python2.7 virtual environment. Then `pip install -r requirements.txt`.

Copy the config template to `config.yaml` and fill in the blanks. Agent output can be kafka, stdout, or both. Analyst output can be only Kafka for now.

`sudo python agent.py`

and

`python analyst.py`
