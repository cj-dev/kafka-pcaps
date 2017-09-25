from pprint import pprint

import yaml
from scapy.all import sniff

with open("config.yml") as cf:
    config = yaml.load(cf, Loader=yaml.CLoader)

print "Running with config \n{0}".format(yaml.dump(config))

captured = sniff(filter='tcp', count=2)
print captured.nsummary()
