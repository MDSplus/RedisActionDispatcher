#!/usr/bin/python3 -u
import redis
import os
import sys
import MDSplus


if len(sys.argv) != 5 and len(sys.argv) != 6:
    print('usage: python display_logs.py <tree> <shot> <ident> <action full path> [redis server]')
    sys.exit(0)
if len(sys.argv) == 5:
    red = redis.Redis(host='localhost')
else:
    red = redis.Redis(host=sys.argv[5])

treeName = sys.argv[1].upper()
shotStr = sys.argv[2]
ident = sys.argv[3]
actionPath = sys.argv[4].upper()

if actionPath != 'ANY':
    tree = MDSplus.Tree(treeName, int(shotStr))
    actionPath = tree.getNode(actionPath).getFullPath()
    print(actionPath[0])
    print(actionPath[1])

print(treeName, shotStr, ident, actionPath)
pubsub = red.pubsub()
pubsub.subscribe('ACTION_DISPATCHER_PUBSUB')
while True:
    message = pubsub.get_message(timeout=100)
    if message == None or not 'data' in message.keys() or not isinstance(message['data'], bytes):
        continue
    msg = message['data'].decode('utf8')
    parts = msg.split('+')
    if parts[0] == treeName and parts[1] == shotStr and parts[2] == ident:
        if actionPath.upper() == 'ANY' or actionPath == parts[3]:
            print(parts[5]) 
 
