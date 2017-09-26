from kafka import KafkaProducer

class KafkaWriter(object):

    def __init__(self, config):
        host = config.get('host', '127.0.0.1')
        port = config.get('port', 9092)
        server_string = "{host}:{port}".format(host=host, port=port)
        self.producer = KafkaProducer(bootstrap_servers=server_string)
    
    def write(self, packet, topic):
        pass
        # future = self.producer.send(topic, b'test_message')
        # result = future.get(timeout=30)

class StdoutWriter(object):

    def __init__(self):
        pass
    
    def write(self, packet):
        print "Summary ", packet.sent_time, packet.summary()
        print "Source and dest ", packet['IP'].src, packet['IP'].dst
        print "Show() ", packet.show()

class Dispatcher(object):
    
    def __init__(self, config):
        self.writers = self.select_writers(config)

    def select_writers(self, config):
        writers = []

        # Currently, strings or dictionary items in output config list
        for output_method in config['agent']['output']:
            if type(output_method) == str:
                if output_method == "stdout":
                    chosen_writer = StdoutWriter()
            else:
                kafka_config = output_method.get('kafka', False)
                if kafka_config:
                    chosen_writer = KafkaWriter(kafka_config)
                    writers.append(chosen_writer)
            writers.append(chosen_writer)

        return writers

    def write_packet(self, packet):
        for writer in self.writers:
            writer.write(packet)

