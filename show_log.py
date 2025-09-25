#!/usr/bin/python3 -u
import redis
import os
import sys
import MDSplus


if len(sys.argv) != 5 and len(sys.argv) != 6:
    print('usage: python show_log.py <tree> <shot>  <action full path> [redis server]')
    sys.exit(0)
if len(sys.argv) == 5:
    red = redis.Redis(host='localhost')
else:
    red = redis.Redis(host=sys.argv[5])

treeName = sys.argv[1].upper()
shotStr = sys.argv[2]
t = MDSplus.Tree(treeName, -1)
try:
    actionPath = t.getNode(sys.argv[3]).getFullPath()
except:
    print('Cannot find node')
    sys.exit(0)

log = red.hget('ACTION_LOG:'+treeName+':'+shotStr, actionPath)
print(log.decode('utf-8'))
