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
    red.hset('DISPATCH_DEFAULT', 'TREE', parts[1].upper())
    red.hset('DISPATCH_DEFAULT', 'SHOT', parts[2].upper())
    red.publish('ACTION_DISPATCHER_COMMANDS', 'create_pulse:'+parts[1].upper()+':'+parts[2].upper())
elif len(parts) == 1 and parts[0].lower() == 'build_tables':
    currTree = red.hget('DISPATCH_DEFAULT', 'TREE')
    currShot = red.hget('DISPATCH_DEFAULT', 'SHOT')
    red.publish('ACTION_DISPATCHER_COMMANDS', 'build_tables:'+currTree+':'+currShot)
elif len(parts) == 2 and parts[0].lower() == 'do_phase':
    currTree = red.hget('DISPATCH_DEFAULT', 'TREE')
    currShot = red.hget('DISPATCH_DEFAULT', 'SHOT')
    monPubsub = red.pubsub()
    monPubsub.subscribe('DISPATCH_MONITOR_PUBSUB')
    red.publish('ACTION_DISPATCHER_COMMANDS', 'do_phase:'+currTree+':'+currShot+':'+parts[1].upper())
    while(True):
        message = monPubsub.get_message(timeout=100)
        if message == None:
            continue
        if not 'data' in message.keys() or not isinstance(message['data'], bytes):
                continue
        msg = message['data'].decode('utf8')
        print(msg)
        parts = msg.split('+')
        if parts[0] == 'END_PHASE':
            break
elif len(parts) == 6 and parts[0].lower() == 'do_sequence':
    red.publish('ACTION_DISPATCHER_COMMANDS', 'do_sequence:'+parts[1]+':'+parts[2]+':'+parts[3]+':'+parts[4]+':'+parts[5])
elif len(parts) == 3 and parts[0].lower() == 'server_restart':
    red.publish('ACTION_SERVER_PUBSUB:'+parts[1], 'RESTART+'+parts[2])
elif len(parts) == 3 and parts[0].lower() == 'server_stop':
    red.publish('ACTION_SERVER_PUBSUB:'+parts[1], 'STOP+'+parts[2])
elif len(parts) == 3 and parts[0].lower() == 'server_abort':
    red.publish('ACTION_SERVER_PUBSUB:'+parts[1], 'ABORT+'+parts[2])
elif len(parts) == 3 and parts[0].lower() == 'server_quit':
    serverStatus = red.hget('ACTION_SERVER_ACTIVE:'+parts[1], parts[2])
    if serverStatus != 'ON':
        print('Server was not active')
    else:
        print('Server was not active')
    red.publish('ACTION_SERVER_PUBSUB:'+parts[1], 'QUIT+'+parts[2])
else:
    print("Usage: python dispatcher_commands.py redis_sever <command>")
    print('Available commands: ')
    print('quit: quit dispatcher')
    print('abort: abort current sequence')
    print('create_pulse <tree> <shot>: create pulse file')
    print('build_tables : create dispatch tables for the specified pulse file')
    print('do_phase <phase>: execute the actions defined for that phase')
    print('do_sequence <tree> <shot> <phase> <start> <end>: execute the sequential actions and the depending ones defined for that phase in the specified sequence range')
    print('server_restart <server class> <server id>: abort pending actions and restart server')
    print('server_abort <server class> <server id> <action path>: abort specified actions')
    print('server_stop <server class> <server id>: stop server (finish pending actions) ')
    print('server_quit <server class> <server id>: abrupt quit server')

