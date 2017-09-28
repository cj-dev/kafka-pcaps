import yaml
import StringIO
from kafka import KafkaConsumer
from scapy.all import Ether, IP, hexdump, rdpcap

if __name__ == "__main__":

    with open("config.yml") as cf:
        config = yaml.load(cf, Loader=yaml.CLoader)

    # Will I ever have more than one input? probably not...
    for input_method in config['analyst']['input']:
        kafka_config = input_method.get('kafka', None)

    if not kafka_config:
        print "Go define a config in the yaml"
        exit(1)

    host = config.get('host', '127.0.0.1')
    port = config.get('port', 9092)
    server_string = "{host}:{port}".format(host=host, port=port)

    consumer = KafkaConsumer('dns_packets', bootstrap_servers=server_string)
    for message in consumer:
        packet_bytes = message.value
        print repr(packet_bytes)
        packet = Ether(packet_bytes)
        packet.show()
