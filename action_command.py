#!/usr/bin/python3 -u
import redis
import os
import sys


if len(sys.argv) != 3:
    print('usage: python action_command.py <command>  [redis server]')
    sys.exit(0)
if len(sys.argv) == 1:
    red = redis.Redis(host='localhost')
else:
    red = redis.Redis(host=sys.argv[2])

red.publish('ACTION_DISPATCHER_COMMANDS', sys.argv[1])
