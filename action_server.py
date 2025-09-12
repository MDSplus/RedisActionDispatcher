#!/usr/bin/python3 -u
import MDSplus
import redis
import time
import random
import threading
import traceback
#import multiprocessing as mp
from multiprocessing import Process, Pipe 
import sys
import os
import time
import signal
import atexit
import socket

from datetime import datetime

lastTree = ''
lastShot = 0

def closeStdout(signum, frame):
    sys.stdout.flush()
    sys.exit()

def reportExit(red, ident, id):
    red.hset('ACTION_SERVER_ACTIVE:'+ident, id, 'OFF')
    os._exit(0)

def makeASCII(txt):
    outTxt = ''
    for char in txt:
        if ord(char) <= 127:
            outTxt += char
    return outTxt

def execute(treeName, shot, actionPath):
    signal.signal(signal.SIGTERM, closeStdout)
    outFd = open(str(os.getpid())+'Log.out',  'a')
    os.dup2(outFd.fileno(), 1)
   # os.dup2(outFd.fileno(), 2)
    try:
        tree = MDSplus.Tree(treeName, shot)
        node = tree.getNode(actionPath)
        task = node.getData().getTask()
        date = datetime.today().strftime('%a %b %d %H:%M:%S CET %Y')
        print(date + '  Doing '+ actionPath)
        if isinstance(task, MDSplus.Program) or isinstance(task, MDSplus.Procedure or isinstance(task, MDSplus.Routine)):
            tree.tcl('do '+ actionPath)
            status = 'Success'
        elif isinstance(task, MDSplus.Method):
            status = task.getObject().doMethod(task.getMethod())
            if status == None:
                status = 'Success'
        else:
            status = int(task.data())
            if status % 2 != 0:
                status = 'Success'
            else:
                status = 'Failure'
    except Exception as exc:
            status = '0'
            traceback.print_exc(exc)
    date = datetime.today().strftime('%a %b %d %H:%M:%S CET %Y')
    print(date + ' Done '+ actionPath)
    statusFile = open(str(os.getpid()) + 'Status.out', 'w')
    statusFile.write(status)
    statusFile.flush()
    statusFile.close()
    #os.fsync(outFd)
    outFd.flush()
    outFd.close()

def handleExecute(treeName, shot, actionPath, timeout, red, ident, serverId, actionNid, notifyDone):
        p = Process(target=execute, args = (treeName, shot, actionPath, ))
        red.hset('ACTION_INFO:'+treeName+':'+str(shot)+':'+ident, actionPath, 'DOING')
        red.publish('DISPATCH_MONITOR_PUBSUB', 'DOING+'+ treeName+'+'+str(shot)+'+'+ident+'+'+str(serverId)+'+'+actionPath+'+'+actionNid)
        red.hset('ACTION_STATUS:'+treeName+':'+str(shot), actionPath, 'None')
       #self.processHash[self.treeName+':' + str(self.shot) + self.actionPath] = p
        p.start()
        pid = p.pid
        if timeout == 0:
            timeoutSecs = 1000000
        else:
            timeoutSecs = int(timeout)
        for i in range(timeoutSecs):
            p.join(0.5)
            if p.exitcode != None:
                break
            else:
                if red.hget('ABORT_REQUESTS:'+ident, actionPath) == b'1':
                    red.hset('ACTION_STATUS:'+treeName+':'+str(shot), actionPath, 'Aborted')
                    break
            
        if p.exitcode == None: #not yet terminated
            p.terminate()

        logFile = open(str(pid) + 'Log.out', 'r')
        log = logFile.read()
        logFile.close()
        os.system('rm '+str(pid) + 'Log.out')
        print("LOG:")
        print(log)
        print("*****")
        try:
            statusFile =  open(str(pid) + 'Status.out', 'r')
            status = statusFile.read()
            statusFile.close()
            os.system('rm '+str(pid) + 'Status.out')
        except:
            status = 'Aborted'

        red.hincrby('ACTION_SERVER_DOING:'+ident, serverId, -1)
        if notifyDone:
            st = treeName +'+'+str(shot)+'+'+ident + '+' + actionPath + '+'+status
            st += '+'+makeASCII(log)
            red.publish('ACTION_DISPATCHER_PUBSUB',st)
        else:
            red.hset('ACTION_STATUS:'+treeName+':'+str(shot), actionPath, status)
 
        red.hset('ABORT_REQUESTS:'+ident, actionPath, '0')
        red.hset('ACTION_INFO:'+treeName+':'+str(shot)+':'+ident, actionPath, 'DONE')
        red.publish('DISPATCH_MONITOR_PUBSUB', 'DONE+'+ treeName+'+'+str(shot)+'+'+ident+'+'+str(serverId)+'+'+actionPath+'+'+actionNid+'+'+status)




class WorkerAction:
    def __init__(self, treeName, shot, actionPath, actionNid, timeout,  ident, serverId, red, notifyDone = True):
        self.treeName = treeName
        self.shot = shot
        self.actionPath = actionPath
        if actionNid == '0':
            try:
                t = MDSplus.Tree(treeName, int(shot))
                self.actionNid = str(t.getNode(actionPath).getNid())
            except:
                print("Failed to retrieve NID for redispatch")
                self.actionNid = '0'
        else:
            self.actionNid = actionNid
        self.timeout = timeout
        self.red = red
        self.ident = ident
        self.serverId = serverId
        self.notifyDone = notifyDone
    
    def spawn(self):
        self.red.hset('ABORT_REQUESTS:'+self.ident, self.actionPath, '0')
        p = Process(target=handleExecute, args = (self.treeName, self.shot, self.actionPath, self.timeout, self.red, self.ident, self.serverId, self.actionNid, self.notifyDone,))
        p.start()



class ActionServer:
    def __init__(self, ident, serverId, red):
        self.ident = ident
        self.serverId = serverId
        self.updPubsub = red.pubsub()
        self.updPubsub.subscribe('ACTION_SERVER_PUBSUB:'+ident)
        print('Listening to '+'ACTION_SERVER_PUBSUB:'+ident)
        self.pendingPids = []
        self.updateMutex = threading.Lock()
        self.updateEvent = threading.Event()
        self.red = red
        self.stopped = False
        red.hset('ACTION_SERVER_HEARTBEAT:'+self.ident, self.serverId, '0')
        red.hset('ACTION_SERVER_DOING:'+self.ident, self.serverId, '0')
        ip = socket.gethostbyname(socket.gethostname())
        red.hset('ACTION_SERVER_IP:'+self.ident, self.serverId, ip)


    def handleDo(self):
        global lastTree, lastShot
        while True:
            toDoMsg = self.red.lpop('ACTION_SERVER_TODO:'+self.ident)
            if toDoMsg == None:
                break
            items = toDoMsg.decode('utf-8').split('+')
            if len(items) != 5 and len(items) != 6:
                print('INTERNAL ERROR. Wrong server message: '+toDoMsg)
                continue
            timeout = int(items[4])
            if len(items) == 6:
                notifyDone = (items[5] == '1')
            else:
                notifyDone = True
            lastTree = items[0]
            lastShot = items[1]
            self.red.hincrby('ACTION_SERVER_DOING:'+self.ident, self.serverId, 1)
            
            worker = WorkerAction(items[0], int(items[1]), items[2], items[3], timeout, self.ident, self.serverId, self.red, notifyDone)
            worker.spawn()

    def handleCommands(self):
        if self.stopped:
            return
        self.handleDo()
        while True:
            message = self.updPubsub.get_message(timeout=100)
            if message == None:
                continue
            if not 'data' in message.keys() or not isinstance(message['data'], bytes):
                continue
            msg = message['data'].decode('utf8')
            if len(msg) == 2 and msg.upper() == 'DO':
                self.handleDo()
            elif len(msg) > 5 and msg[:5].upper() == 'ABORT':
                items = msg.split('+')
                if len(items) != 2:
                    print('Unexpected message: '+msg)
                else:
                    self.red.hset('ABORT_REQUESTS:'+self.ident, items[1], '1')
            elif msg.upper()[:7] == 'RESTART':
                items = msg.split('+')
                if len(items) != 2:
                    print('Internal error. Wrong command message: '+msg)
                    continue
                if items[1] == self.serverId:
                    actionPaths = self.red.hkeys('ACTION_INFO:'+lastTree+':'+lastShot+':'+self.ident)
                    for actionPath in actionPaths:
                        if self.red.hget('ACTION_INFO:'+lastTree+':'+ lastShot+':'+self.ident, actionPath).decode('utf-8') == ('DOING '+str(self.serverId)):
                            self.red.hset('ABORT_REQUESTS:'+self.ident, actionPath, '1')
                    self.stopped = False
            elif msg.upper()[:4] == 'STOP': 
                items = msg.split('+')
                if len(items) != 2:
                    print('Internal error. Wrong command message: '+msg)
                    continue
                if items[1] == self.serverId:
                    self.stopped = True
            elif msg.upper()[:4] == 'QUIT': 
                items = msg.split('+')
                if len(items) != 2:
                    print('Internal error. Wrong command message: '+msg)
                    continue
                if items[1] == self.serverId:
                    print('Server exit')
                    red.hset('ACTION_SERVER_ACTIVE:'+ident, id, 'OFF')
                    os._exit(0)
                    #sys.exit(0)
            elif msg.upper()[:9] == 'HEARTBEAT':
                items = msg.split('+')
                if len(items) != 2:
                    print('Internal error. Wrong command message: '+msg)
                    continue
                if items[1] == self.serverId:
                    self.red.hincrby('ACTION_SERVER_HEARTBEAT:'+self.ident, self.serverId, 1)
            else:
                print('INVALID MESSAGE: '+msg)




def reportServerOn(red, ident, id):
    while True:
        red.hset('ACTION_SERVER_ACTIVE:'+ident, id, 'ON')
        time.sleep(1)



if len(sys.argv) != 3 and len(sys.argv) != 4:
    print('usage: python action_server.py <server class> <server id> [redis server]')
    sys.exit(0)
if len(sys.argv) == 3:
    red = redis.Redis(host='localhost')
else:
    red = redis.Redis(host=sys.argv[3])

ident = sys.argv[1]
id = sys.argv[2]
print('Action server started. Server class: '+ident+', Server Id: '+id)
act = ActionServer(ident, id, red)
#atexit.register(reportExit, red, ident, id)
#red.hset('ACTION_SERVER_ACTIVE:'+ident, id, 'ON')
thread = threading.Thread(target = reportServerOn, args = (red, ident, id,))
thread.start()

act.handleCommands()
   














                        





                





