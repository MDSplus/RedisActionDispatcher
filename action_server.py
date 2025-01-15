#!/usr/bin/python3 -u
import MDSplus
import redis
import time
import random
import threading
import traceback
from multiprocessing import Process, Pipe 
import sys
import os
import time
import signal


def closeStdout(signum, frame):
    sys.stdout.flush()
    sys.exit()

def execute(treeName, shot, actionPath):
    signal.signal(signal.SIGTERM, closeStdout)
    outFd = open(str(os.getpid())+'Log.out', 'w')
    os.dup2(outFd.fileno(), 1)
    statusFile = open(str(os.getpid()) + 'Status.out', 'w')
    try:
        tree = MDSplus.Tree(treeName, shot)
        node = tree.getNode(actionPath)
        task = node.getData().getTask()
        print('Doing '+ actionPath)
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
    print('Done '+ actionPath)
    statusFile.write(status)
    statusFile.flush()
    statusFile.close()
    outFd.flush()
    outFd.close()


class WorkerAction(threading.Thread):
    def __init__(self, treeName, shot, actionPath, timeout, updateMutex, processHash, ident, red):
        super(WorkerAction, self).__init__()
        self.treeName = treeName
        self.shot = shot
        self.actionPath = actionPath
        self.timeout = timeout
        self.updateMutex = updateMutex
        self.red = red
        self.processHash = processHash
        self.serverClass = ident

    def run(self):

        p = Process(target=execute, args = (self.treeName, self.shot, self.actionPath, ))
        self.updateMutex.acquire()
        self.red.hset('ACTION_INFO:'+self.treeName+':'+str(self.shot), self.actionPath, 'DOING')
        self.processHash[self.treeName+':' + str(self.shot) + self.actionPath] = p
        self.updateMutex.release()
        p.start()
        pid = p.pid
        if self.timeout == 0:
            p.join()
        else:
            timeoutSecs = int(self.timeout)
            for i in range(timeoutSecs):
                p.join(1)
                if p.exitcode != None:
                    break
            if p.exitcode == None: #not yet terminated
                p.terminate()

        logFile = open(str(pid) + "Log.out", "r")
        log = logFile.read()
        logFile.close()
        print(log)
        try:
            statusFile =  open(str(pid) + "Status.out", "r")
            status = statusFile.read()
            statusFile.close()
        except:
            status = 'Aborted'

        self.updateMutex.acquire()
        self.processHash[self.treeName+':' + str(self.shot) + self.actionPath] = None
        self.red.publish('ACTION_DISPATCHER_PUBSUB', self.treeName +'+'+str(self.shot)+'+'+self.serverClass + '+' + self.actionPath+'+'+status+'+'+log)
        self.red.hset('ACTION_INFO:'+self.treeName+':'+str(self.shot), self.actionPath, 'DONE')
        self.updateMutex.release()

class WorkerCommands(threading.Thread):
    def __init__(self, actionServer):
        super(WorkerCommands, self).__init__()
        self.actionServer = actionServer
    
    def run(self):
        self.actionServer.handleCommands()



class ActionServer:
    def __init__(self, ident, serverId, red):
        self.ident = ident
        self.serverId = serverId
        self.updPubsub = red.pubsub()
        self.updPubsub.subscribe('ACTION_SERVER_PUBSUB:'+ident)
        self.cmdPubsub = red.pubsub()
        self.cmdPubsub.subscribe('ACTION_SERVER_COMMANDS:'+ident)
        self.pendingPids = []
        self.updateMutex = threading.Lock()
        self.updateEvent = threading.Event()
        self.red = red
        self.processHash = {}
        self.stopped = False
        red.set('ACTION_SERVER_HEARTBEAT:'+self.ident, 0)

    def handleDo(self):
        if self.stopped:
            return
        while True:
            message = self.updPubsub.get_message(timeout=100)
            if message == None:
                continue
            if not 'data' in message.keys() or not isinstance(message['data'], bytes):
                continue
            msg = message['data'].decode('utf8')
            if msg.upper() != 'DO':
                print('INTERNAL ERROR: Wrong Message '+msg)
                return
            while True:
                toDoMsg = self.red.lpop('ACTION_SERVER_TODO:'+self.ident)
                if toDoMsg == None:
                    break
                items = toDoMsg.split('+')
                if len(items) != 3 and len(items) != 4:
                    print('INTERNAL ERROR. Wrong server message: '+toDoMsg)
                    continue
                if len(items) == 3:
                    timeout = None
                else:
                    timeout = int(items[3])
                worker = WorkerAction(items[0], int(items[1]), items[2], timeout, self.updateMutex, self.processHash, self.ident, self.red)
                worker.start()

    def handleAbort(self, treeName, shot, actionPath):
        self.updateMutex.acquire()
        try:
            p = self.processHash[treeName+':' + str(shot) + actionPath]
            if p != None:
                p.terminate()
        except:
            pass
        self.updateMutex.release()

    def handleRestart(self):
        self.updateMutex.acquire()
        for k in self.processHash.keys():
            p = self.processHash[k]
            if p != None:
                p.terminate()
        self.updateMutex.release()
        self.stopped = False

    def handleStop(self):
        self.stopped = True

    def handleHeartBeat(self):
        self.red.hincrby('ACTION_SERVER_HEARTBEAT:'+self.ident, self.serverId, 1)

    def handleCommands(self):
        while True:
            message = self.cmdPubsub.get_message(timeout=100)
            if message == None:
                continue
            if not 'data' in message.keys() or not isinstance(message['data'], bytes):
                continue
            msg = message['data'].decode('utf8')
            print('RECEIVED COMMAND: ', msg)
            if msg.upper()[:5] != 'ABORT':
                items = msg.split('+')
                if len(items) != 4:
                    print('Internal error. Wrong command message: '+msg)
                    continue
                self.handleAbort(items[1], int(items[2]), items[3]) 
            elif msg.upper()[:7] == 'RESTART':
                items = msg.split('+')
                if len(items) != 2:
                    print('Internal error. Wrong command message: '+msg)
                    continue
                if items[2] == self.serverId:
                    self.handleRestart()
            elif msg.upper()[:4] == 'STOP':
                items = msg.split('+')
                if len(items) != 2:
                    print('Internal error. Wrong command message: '+msg)
                    continue
                if items[2] == self.serverId:
                    self.handleStop()
            elif msg.upper()[:9] == 'HEARTBEAT':
                items = msg.split('+')
                if len(items) != 2:
                    print('Internal error. Wrong command message: '+msg)
                    continue
                if items[2] == self.serverId:
                    self.handleHeartBeat()
            else:
                print('INVALID MESSAGE: '+msg)

    def startHandleCommands(self):
        worker = WorkerCommands(self)
        worker.start()


if len(sys.argv) != 3 and len(sys.argv) != 4:
    print('usage: python action_server.py <server class> <server id> [redis server]')
    sys.exit(0)
if len(sys.argv) == 3:
    red = redis.Redis(host='localhost')
else:
    red = redis.Redis(host=sys.argv[3])
    
act = ActionServer(sys.argv[1], sys.argv[2], red)
act.startHandleCommands()
act.handleDo()
   














                        





                





