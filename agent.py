from pprint import pprint

import yaml
from scapy.all import sniff

from writers.writers import KafkaWriter, StdoutWriter

def write_packet(writers, packet):
    pass


def select_writers(config):
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

if __name__ == "__main__":
    with open("config.yml") as cf:
        config = yaml.load(cf, Loader=yaml.CLoader)

    print "Running with config \n{0}".format(yaml.dump(config))

    select_writers(config)

    captured = sniff(filter='tcp', count=2)
    print captured.nsummary()

