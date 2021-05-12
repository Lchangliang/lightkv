#!/bin/python3

import yaml

def load_conf(yaml_file):
    file = open(yaml_file, 'r', encoding="utf-8")
    file_data = file.read()
    file.close()
    conf = yaml.load(file_data, yaml.FullLoader)
    return conf