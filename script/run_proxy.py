#!/bin/python3

import os
import subprocess
import load_config

conf = load_config.load_conf("conf/conf.yaml")
server_num = len(conf['storage'])
raft_peer = ""


exec_command = "nohup ./bin/lightkv_client > log/client.log 2>&1 &"
subprocess.call(exec_command, shell=True)
