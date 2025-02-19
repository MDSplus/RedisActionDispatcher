#!/usr/bin/python3 -u
import redis
import os
import sys

parts = []
if len(sys.argv) > 2:
    red = redis.Redis(host=sys.argv[1])
    parts = sys.argv[2:]
    print(parts[0])

if len(parts) == 1 and parts[0].lower() == 'quit':
    red.publish('ACTION_DISPATCHER_COMMANDS', 'QUIT')
if len(parts) == 1 and parts[0].lower() == 'abort':
    red.publish('ACTION_DISPATCHER_COMMANDS', 'ABORT')
elif len(parts) == 3 and parts[0].lower() == 'create_pulse':
    red.publish('ACTION_DISPATCHER_COMMANDS', 'create_pulse:'+parts[1]+':'+parts[2])
elif len(parts) == 3 and parts[0].lower() == 'build_tables':
    red.publish('ACTION_DISPATCHER_COMMANDS', 'build_tables:'+parts[1]+':'+parts[2])
elif len(parts) == 4 and parts[0].lower() == 'do_phase':
    red.publish('ACTION_DISPATCHER_COMMANDS', 'do_phase:'+parts[1]+':'+parts[2]+':'+parts[3])
elif len(parts) == 6 and parts[0].lower() == 'do_sequence':
    red.publish('ACTION_DISPATCHER_COMMANDS', 'do_sequence:'+parts[1]+':'+parts[2]+':'+parts[3]+':'+parts[4]+':'+parts[5])
elif len(parts) == 3 and parts[0].lower() == 'server_restart':
    red.publish('ACTION_SERVER_PUBSUB:'+parts[1], 'RESTART+'+parts[2])
elif len(parts) == 3 and parts[0].lower() == 'server_stop':
    red.publish('ACTION_SERVER_PUBSUB:'+parts[1], 'STOP+'+parts[2])
elif len(parts) == 3 and parts[0].lower() == 'server_abort':
    red.publish('ACTION_SERVER_PUBSUB:'+parts[1], 'ABORT+'+parts[2])
elif len(parts) == 3 and parts[0].lower() == 'server_quit':
    red.publish('ACTION_SERVER_PUBSUB:'+parts[1], 'QUIT+'+parts[2])
else:
    print("Usage: python dispatcher_commands.py redis_sever <command>")
    print('Available commands: ')
    print('exit: exit this program')
    print('quit: quit dispatcher')
    print('abort: abort current sequence')
    print('create_pulse <tree> <shot>: create pulse file')
    print('build_tables <tree> <shot>: create dispatch tables for the specified pulse file')
    print('do_phase <tree> <shot> <phase>: execute the actions defined for that phase')
    print('do_sequence <tree> <shot> <phase> <start> <end>: execute the sequential actions and the depending ones defined for that phase in the specified sequence range')
    print('server_restart <server class> <server id>: abort pending actions and restart server')
    print('server_abort <server class> <server id> <action path>: abort specified actions')
    print('server_stop <server class> <server id>: stop server (finish pending actions) ')
    print('server_quit <server class> <server id>: abrupt quit server')

