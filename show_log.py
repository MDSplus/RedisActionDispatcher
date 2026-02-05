#!/usr/bin/env python
import argparse
import redis_connector
import redis
import os
import sys
import logging
import MDSplus

parser = argparse.ArgumentParser()
redis_connector.add_redis_args(parser)

parser.add_argument("tree", type=str, default="test", help="Name of Tree")
parser.add_argument("shot", type=int, default=0, help="Shot number")
parser.add_argument("action", type=str, default="act1", help="Path of action to display")
parser.add_argument("--log-level", default="INFO", choices=["DEBUG","INFO","WARNING","ERROR","CRITICAL"], help="Logging level")

args = parser.parse_args()

logging.basicConfig(level=getattr(logging, args.log_level))

red = redis_connector.connect_redis_from_args(args)
logging.info("Redis connected: %s", red.ping())

treeName = args.tree
shot = args.shot
t = MDSplus.Tree(treeName, shot)
try:
    actionPath = t.getNode(args.action).getFullPath()
except:
    logging.error('Cannot find node: %s', args.action)
    sys.exit(1)

log = red.hget('ACTION_LOG:'+treeName+':'+str(shot), actionPath)
if log:
    print(log.decode('utf-8'))
else:
    logging.warning('No log found for %s:%s %s', treeName, shot, actionPath)
