#!/bin/python3

import os
import sys
import subprocess
import load_config

port = ""
if len(sys.argv) == 2:
    port = sys.argv[1]

conf = load_config.load_conf("conf/conf.yaml")
server_num = len(conf['storage'])

# print(raft_peer)
for i in range(server_num):
    if port == "" or port == str(conf['storage'][i]['port']):
        exec_command = "nohup ./bin/lightkv_server --usercode_in_pthread=true --port=" + str(conf['storage'][i]['port'])  \
            + " --proxy_port=" + str(conf['proxy']['port']) + " > log/server" + str(i) + ".log 2>&1 &"
        subprocess.call(exec_command, shell=True)