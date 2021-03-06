from pprint import pprint

import yaml
from scapy.all import sniff

from writers.writers import KafkaWriter, StdoutWriter, Dispatcher


if __name__ == "__main__":
    with open("config.yml") as cf:
        config = yaml.load(cf, Loader=yaml.CLoader)

    print "Running with config \n{0}".format(yaml.dump(config))

    dispatcher = Dispatcher(config)
    
    capture_ifaces = config['agent']['interfaces']
    captured = sniff(iface=capture_ifaces, filter='ip', lfilter=lambda x: x.haslayer('IP'),
            prn=dispatcher.write_packet, store=0)
    
