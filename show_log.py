#!/usr/bin/env python
import argparse
import redis_connector
import redis
import os
import sys
import MDSplus

parser = argparse.ArgumentParser()
redis_connector.add_redis_args(parser)

parser.add_argument("tree", type=str, default="test", help="Name of Tree")
parser.add_argument("shot", type=int, default=0, help="Shot number")
parser.add_argument("action", type=str, default="act1", help="Path of action to display")

args = parser.parse_args()

red = redis_connector.connect_redis_from_args(args)
print("Connected:", red.ping())

treeName = args.tree
shot = args.shot
t = MDSplus.Tree(treeName, shot)
try:
    actionPath = t.getNode(args.action).getFullPath()
except:
    print('Cannot find node')
    sys.exit(0)

log = red.hget('ACTION_LOG:'+treeName+':'+str(shot), actionPath)
print(log.decode('utf-8'))
