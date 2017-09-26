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
        # write packet to kafka
        pass
