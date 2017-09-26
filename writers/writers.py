from kafka import KafkaProducer
from scapy.all import hexdump

class KafkaWriter(object):

    def __init__(self, config):
        host = config.get('host', '127.0.0.1')
        port = config.get('port', 9092)
        server_string = "{host}:{port}".format(host=host, port=port)
        self.producer = KafkaProducer(bootstrap_servers=server_string)
    
    def write(self, packet):
        if packet.haslayer('DNS'):
            future = self.producer.send('dns_packets', str(packet))
        else:
            future = self.producer.send('catchall', str(packet))
        result = future.get(timeout=30)

class StdoutWriter(object):

    def __init__(self, config):
        self.verbosity = config.get('verbosity', 0)
    
    def write(self, packet):
        print "Timestamp {time}, {summary}".format(
                time=packet.time, summary=packet.summary())
        print "Src IP: {src}, Dst IP: {dst}".format(
                src=packet['IP'].src, dst=packet['IP'].dst)
        if self.verbosity >= 1:
            print hexdump(packet)

class Dispatcher(object):
    
    def __init__(self, config):
        self.writers = self._select_writers(config)

    def _select_writers(self, config):
        writers = []

        for output_method in config['agent']['output']:
            stdout_config = output_method.get('stdout', None)
            if stdout_config:
                chosen_writer = StdoutWriter(stdout_config)
            kafka_config = output_method.get('kafka', None)
            if kafka_config:
                chosen_writer = KafkaWriter(kafka_config)
            writers.append(chosen_writer)

        return writers

    def write_packet(self, packet):
        for writer in self.writers:
            writer.write(packet)

