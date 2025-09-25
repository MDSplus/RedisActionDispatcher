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
import threading 
from datetime import datetime
import argparse

lastTree = ''
lastShot = 0

lastId = 0

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

def executeProcess(treeName, shot, actionPath):
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
            elif status % 2 != 0:
                status = 'Success'
            else:
                status = 'Failure'
        else:
            status = int(task.data())
            if status % 2 != 0:
                status = 'Success'
            else:
                status = 'Failure'
    except Exception as exc:
            status = 'Failure'
            traceback.print_exc(exc)
    date = datetime.today().strftime('%a %b %d %H:%M:%S CET %Y')
    print(date + ' Done '+ actionPath)
    statusFile = open(str(os.getpid()) + 'Status.out', 'w')
#    statusFile.write('PERDINDIRIDINA')
    statusFile.write(status)
    statusFile.flush()
    statusFile.close()
    #os.fsync(outFd)
    outFd.flush()
    outFd.close()

def handleExecuteProcess(treeName, shot, actionPath, timeout, red, ident, serverId, actionNid, notifyDone):
        p = Process(target=executeProcess, args = (treeName, shot, actionPath, ))
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
            if status == None or len(status) == 0:
                status = 'Unknown Error'
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

def execute(treeName, shot, actionPath, tid, isSequential):
    if isSequential:
        originalStdoutFd = os.dup(1)  # duplicate fd 1
        outFd = open(str(tid)+'Log.out',  'w')
        os.dup2(outFd.fileno(), 1)
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
            elif status % 2 != 0:
                status = 'Success'
            else:
                status = 'Failure'
        else:
            status = int(task.data())
            if status % 2 != 0:
                status = 'Success'
            else:
                status = 'Failure'
    except Exception as exc:
            status = 'Failure'
            traceback.print_exc(exc)
  
    if isSequential:
        outFd.flush()
        os.fsync(outFd)
        outFd.close()
        os.dup2(originalStdoutFd, 1)

    date = datetime.today().strftime('%a %b %d %H:%M:%S CET %Y')
    print(date + ' Done '+ actionPath)
    statusFile = open(str(tid) + 'Status.out', 'w')
    statusFile.write(status)
    statusFile.flush()
    statusFile.close()



def handleExecute(treeName, shot, actionPath, timeout, red, ident, serverId, actionNid, notifyDone, tid, isSequential, mutex):
        t = threading.Thread(target=execute, args = (treeName, shot, actionPath, tid, isSequential,))
        red.hset('ACTION_INFO:'+treeName+':'+str(shot)+':'+ident, actionPath, 'DOING')
        red.publish('DISPATCH_MONITOR_PUBSUB', 'DOING+'+ treeName+'+'+str(shot)+'+'+ident+'+'+str(serverId)+'+'+actionPath+'+'+actionNid)
        red.hset('ACTION_STATUS:'+treeName+':'+str(shot), actionPath, 'None')
        if isSequential:
            mutex.acquire()
        t.start()
        if timeout == 0:
            timeoutSecs = 1000000
        else:
            timeoutSecs = int(timeout)
        for i in range(timeoutSecs):
            t.join(0.5)
            if not t.isAlive():
                break
            else:
                if red.hget('ABORT_REQUESTS:'+ident, actionPath) == b'1':
                    red.hset('ACTION_STATUS:'+treeName+':'+str(shot), actionPath, 'Aborted')
                    break
#Cannot terminate a thread......      
#        if t.isAlive(): #not yet terminated
#            p.terminate()


        if isSequential:
          mutex.release()
          logFile = open(str(tid) + 'Log.out', 'r')
          log = logFile.read()
          logFile.close()
          os.system('rm '+str(tid) + 'Log.out')
        else:
          log = ''
        if isSequential:
            print("LOG:")
            print(log)
            print("*****")
        try:
            statusFile =  open(str(tid) + 'Status.out', 'r')
            status = statusFile.read()
            statusFile.close()
            os.system('rm '+str(tid) + 'Status.out')
            if status == None or len(status) == 0:
                status = 'Unknown Error'
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
    def __init__(self, treeName, shot, actionPath, actionNid, timeout,  ident, serverId, red, isSequential, isProcess, mutex, notifyDone = True):
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
        self.isSequential = isSequential
        self.isProcess = isProcess
        self.mutex = mutex
    
    def spawn(self):
        global lastId
        tid = threading.current_thread().ident + lastId
        lastId = lastId + 1
        self.red.hset('ABORT_REQUESTS:'+self.ident, self.actionPath, '0')
        if not self.isSequential:  #Parallel
            if self.isProcess:
                p = threading.Thread(target=handleExecuteProcess, args = (self.treeName, self.shot, self.actionPath, self.timeout, self.red, self.ident, self.serverId, self.actionNid, self.notifyDone, ))
            else:
               p = threading.Thread(target=handleExecute, args = (self.treeName, self.shot, self.actionPath, self.timeout, self.red, self.ident, self.serverId, self.actionNid, self.notifyDone, tid, self.isSequential, self.mutex))
            p.start()
        else: #Sequential
            if self.isProcess:
                handleExecuteProcess(self.treeName, self.shot, self.actionPath, self.timeout, self.red, self.ident, self.serverId, self.actionNid, self.notifyDone)
            else:
                handleExecute(self.treeName, self.shot, self.actionPath, self.timeout, self.red, self.ident, self.serverId, self.actionNid, self.notifyDone, tid, self.isSequential, self.mutex)


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


    def handleDo(self, isSequential, isProcess, mutex):
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
            
            worker = WorkerAction(items[0], int(items[1]), items[2], items[3], timeout, self.ident, self.serverId, self.red, isSequential, isProcess, mutex, notifyDone)
            worker.spawn()

    def handleCommands(self, isSequential, isProcess):
        if self.stopped:
            return
        mutex = threading.Lock()

        self.handleDo(isSequential, isProcess, mutex)
        while True:
            message = self.updPubsub.get_message(timeout=100)
            if message == None:
                continue
            if not 'data' in message.keys() or not isinstance(message['data'], bytes):
                continue
            msg = message['data'].decode('utf8')
            if len(msg) == 2 and msg.upper() == 'DO':
                self.handleDo(isSequential, isProcess, mutex)
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
                    self.red.hset('ACTION_SERVER_ACTIVE:'+self.ident, self.serverId, 'OFF')
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

def main(serverClass, serverId, redisServer, sequential, process):
    red = redis.Redis(host=redisServer)
    ident = serverClass
    id = serverId
    print('Action server started. Server class: '+ident+', Server Id: '+id)
    act = ActionServer(ident, id, red)
    #atexit.register(reportExit, red, ident, id)
    #red.hset('ACTION_SERVER_ACTIVE:'+ident, id, 'ON')
    thread = threading.Thread(target = reportServerOn, args = (red, ident, id,))
    thread.start()
    act.handleCommands(sequential != 0, process != 0)
    


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    
    # positional argument
    parser.add_argument("serverClass", help="Server Class")
    parser.add_argument("serverId", help="ServerId")
    parser.add_argument("redisServer", help="REDIS server")
    parser.add_argument("--sequential", type=int, default=1, help="Force Mutual exclusion for log consistency")
    parser.add_argument("--process", type=int, default=0, help="Force Mutual exclusion for log consistency")
    args = parser.parse_args()
    print(args.serverClass, args.serverId, args.redisServer, args.sequential, args.process)
    main(args.serverClass, args.serverId, args.redisServer, args.sequential, args.process)
















                        





                





