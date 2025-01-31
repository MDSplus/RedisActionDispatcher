#!/usr/bin/python3 -u
import redis
import os
import sys
from datetime import datetime
import socket
import threading
import MDSplus

MonitorBeginSequence = '1'
MonitorBuildBegin = '2'
MonitorBuild = '3'
MonitorBuildEnd = '4'
MonitorCheckin = '5'
MonitorDispatched = '6'
MonitorDoing = '7'
MonitorDone = '8'
MonitorEndPhase = '9'
MonitorStartPhase = '10'
MonitorEndSequence = '11'
MonitorServerConnected = '12'
MonitorServerDisconnected = '13'

lastTree = ''
lastShot = 0

def recvall(conn, nBytes):
    recBytes = 0
    retBa = bytearray([])
    while True:
#        print(nBytes, recBytes)
        retBa += conn.recv(nBytes - recBytes)
        recBytes = len(retBa)
        if recBytes == nBytes:
            break
    return bytes(retBa)

def recvHeader(conn):
    lenMsg = recvall(conn,4)
    msgLen = int.from_bytes(lenMsg,'big') - 48
    recvall(conn,6)
    nargMsg = recvall(conn,1)
    nargs = int.from_bytes(nargMsg,'big')
    recvall(conn, 48 - 11)
    print('Message len: '+str(msgLen))
    print('N. Args: '+str(nargs))
    return msgLen, nargs

def sendHeader(conn, msgLen, type = 0):
    outBytes = []
    l = 48 + msgLen
    outBytes += l.to_bytes(4,'big')
    outBytes += bytearray([0,0,0,0])
    outBytes += msgLen.to_bytes(2,'big')
    outBytes += bytearray([0,0,0])
    outBytes += bytearray([type])
    outBytes += bytearray([0x80])
    for i in range(15,48):
        outBytes += bytearray([0])
    conn.send(bytes(outBytes))

def getPhaseDict(propertyFileName):
    propertyF = open(propertyFileName)
    lines =  propertyF.readlines()
    phaseDict = {}
    phaseIdx = 1
    while True:
        phaseName = ''
        phaseCode = -1
        for line in lines:
            property = line.split('=')
            if len(property) != 2:
                continue
            if(property[0].strip() == 'jDispatcher.phase_'+str(phaseIdx)+'.id'):
                phaseCode = int(property[1].strip())
            elif (property[0].strip() == 'jDispatcher.phase_'+str(phaseIdx)+'.name'):
                phaseName = property[1].strip()
        if phaseName != '' and phaseCode != -1:
            phaseDict[phaseName.upper()] = phaseCode
        else:
            break
        phaseIdx += 1
    return phaseDict

def buildMessage(tree, shot, phase, nid, on, mode, serverClass, serverId, retStatus, actionPath, dateStr, errMsg):
    if errMsg == None:
        msg = tree+' '+shot +' '+ str(phaseDict[phase.upper()])+' '+nid+' '+on+' '+mode+' '+serverClass+' '+serverId+' '+retStatus+' '+actionPath+' '+dateStr+';'
    else:
        msg = tree+' '+shot +' '+ str(phaseDict[phase.upper()])+' '+nid+' '+on+' '+mode+' '+serverClass+' '+serverId+' '+retStatus+' '+actionPath+' '+dateStr+'; '+errMsg
    header  = '0 0 0 '+str(len(msg)) 

    return header.encode('utf-8'), msg.encode('utf-8')

def sendMessage(sock, tree, shot, phase, nid, on, mode, serverClass, serverId, retStatus, actionPath, dateStr, errMsg = None):
    header, msg = buildMessage(tree, shot, phase, nid, on, mode, serverClass, serverId, retStatus, actionPath, dateStr, errMsg)
    global lastShot, lastTree
    print('Spedisco: ', msg)
    lastTree = tree
    lastShot = int(shot)
    sock.send(header)
    for i in range(60 - len(header)):
        sock.send(bytes(bytearray([0])))
    sock.send(msg)

def handleNotification(sock):
    print('Going to server connection....')
    monPubsub = red.pubsub()
    monPubsub.subscribe('DISPATCH_MONITOR_PUBSUB')
    while(True):
        message = monPubsub.get_message(timeout=100)
        if message == None:
            continue
        if not 'data' in message.keys() or not isinstance(message['data'], bytes):
                continue
        msg = message['data'].decode('utf8')
        print(msg)
        parts = msg.split('+')
        if parts[0] == 'START_PHASE':
            tree = parts[1]
            shot = parts[2]
            phase = parts[3]
            sendMessage(sock, tree, shot, phase, '0', '1', MonitorStartPhase, 'XX', '0', '0',  'XX', str(datetime.now()))
        elif parts[0] == 'END_PHASE':
            tree = parts[1]
            shot = parts[2]
            phase = parts[3]
            sendMessage(sock, tree, shot, phase, '0', '1', MonitorEndPhase, 'XX', '0', '0',  'XX', str(datetime.now()))
        elif parts[0] == 'BUILD_BEGIN':
            tree = parts[1]
            shot = parts[2]
            phase = parts[3]
            nid = parts[4]
            on = parts[5]
            path = parts[7]
            sendMessage(sock, tree, shot, phase, nid, on, MonitorBuildBegin, 'XX', '0', '0',  path, str(datetime.now()))
        elif parts[0] == 'BUILD_END':
            tree = parts[1]
            shot = parts[2]
            phase = parts[3]
            nid = parts[4]
            on = parts[5]
            path = parts[7]
            sendMessage(sock, tree, shot, phase, nid, on,  MonitorBuildEnd, 'XX', '0', '0',  path, str(datetime.now()))
        elif parts[0] == 'BUILD':
            tree = parts[1]
            shot = parts[2]
            phase = parts[3]
            nid = parts[4]
            on = parts[5]
            path = parts[7]
            sendMessage(sock, tree, shot, phase,  nid, on,  MonitorBuild, 'XX', '0', '0',  path, str(datetime.now()))
        elif parts[0] == 'START_SEQUENCE':
            tree = parts[1]
            shot = parts[2]
            phase = parts[3]
            sendMessage(sock, tree, shot, phase, '0', '1', MonitorBeginSequence, 'XX', '0', '0',  'XX', str(datetime.now()))
        elif parts[0] == 'END_SEQUENCE':
            tree = parts[1]
            shot = parts[2]
            phase = parts[3]
            sendMessage(sock, tree, shot, phase, '0', '1', MonitorEndSequence, 'XX', '0', '0',  'XX', str(datetime.now()))
        elif parts[0] == 'DISPATCHED':
            tree = parts[1]
            shot = parts[2]
            phase = parts[3]
            ident = parts[4]
            actionPath = parts[5]
            nid = parts[6]
            sendMessage(sock, tree, shot, phase, nid, '1', MonitorDispatched, ident, '0', '0', actionPath, str(datetime.now()))
        elif parts[0] == 'DOING':
            tree = parts[1]
            shot = parts[2]
            ident = parts[3]
            serverId = parts[4]
            actionPath = parts[5]
            nid = parts[6]
            sendMessage(sock, tree, shot, phase, nid, '1', MonitorDoing, ident, serverId, '0',  actionPath, str(datetime.now()))
        elif parts[0] == 'DONE':
            tree = parts[1]
            shot = parts[2]
            ident = parts[3]
            serverId = parts[4]
            actionPath = parts[5]
            nid = parts[6]
            status = parts[7]
            if status == 'Success':
                statusCode = '1'
            else:
                statusCode = '0'
            sendMessage(sock, tree, shot, phase, nid, '1', MonitorDone, ident, serverId, statusCode,  actionPath, str(datetime.now()), status)
        else:
            print('Unexpected message: '+parts[0])                


def handleRedispatch(nid):
    node = MDSplus.Tree(lastTree, lastShot).getNode(nid)
    act = node.getData()
    try:
        timeout = act.getTask().getTimeout().data()
    except:
        timeout = 0
    if timeout == None:
        timeout = 0
    path = node.getFullPath()
    ident = node.getData().getDispatch().getIdent().data()
    print('REDISPATCHING', 'ACTION_SERVER_TODO:'+ident, lastTree+'+'+str(lastShot)+'+'+path+'+'+str(nid)+'+'+str(timeout))
    red.lpush('ACTION_SERVER_TODO:'+ident, lastTree+'+'+str(lastShot)+'+'+path+'+'+str(nid)+'+'+str(timeout)+'+0')
    red.publish('ACTION_SERVER_PUBSUB:'+ident, 'DO')

def handleAbort(nid):
    node = MDSplus.Tree(lastTree, lastShot).getNode(nid)
    path = node.getFullPath()
    ident = node.getData().getDispatch().getIdent().data()
    command = 'ABORT+' + path 
    red.publish('ACTION_SERVER_PUBSUB:'+ident, command)


def handleCommands(conn):
    while True:
        msgLen, nargs = recvHeader(conn)
        if msgLen > 0:
            ans = recvall(conn, msgLen)
        cmd = ans.decode('utf-8')
        print('Command: '+cmd)
        sendHeader(conn, 4, 8)
        status =1 
        conn.send(status.to_bytes(4,'big'))
        parts = cmd.split()
        if parts[0] == '@REDISPATCH':
            handleRedispatch(int(parts[1]))
        elif parts[0] == '@ABORT':
            handleAbort(int(parts[1]))



def manageCommands(port):
    serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    host = socket.gethostname()
    serversocket.bind((host, port))
    serversocket.listen(5) # become a server socket, maximum 5 connections
    while True:
        try:
            connection, address = serversocket.accept()
            print('Received connection from ', address)
            msgLen, nargs = recvHeader(connection)
            if msgLen > 0:
                recvall(connection, msgLen)
            sendHeader(connection, 0)
            handleCommands(connection)
        except:
            print('Command Connection terminated')

def handleMonitor(connection):
    try:
        while True:
            print('ATTESA COMANDO')
            msgLen, nargs = recvHeader(connection)
            print('RICEVUTO HEADER '+str(msgLen))
            ans = recvall(connection, msgLen)
            cmd = ans.decode('utf-8')
            print('Command: '+cmd)
            if cmd != 'ServerQAction($,$,$,$,$,$,$,$,$,$,$,$)':
                continue
            msgLen, nargs = recvHeader(connection)
            ans = recvall(connection, 1)
            ipAddr = ''
            ipAddr += str(int.from_bytes(ans, 'big'))
            ans = recvall(connection, 1)
            ipAddr += '.'+str(int.from_bytes(ans, 'big'))
            ans = recvall(connection, 1)
            ipAddr += '.'+str(int.from_bytes(ans, 'big'))
            ans = recvall(connection, 1)
            ipAddr += '.'+str(int.from_bytes(ans, 'big'))
            print('Ip Addr: ', ipAddr)
            msgLen, nargs = recvHeader(connection)
            ans = recvall(connection, msgLen)
            port =  int.from_bytes(ans,'big')
            print('Port: ', str(port))
            for i in range(2,12):
                print('Reading arg  '+str(i))
                msgLen, nargs = recvHeader(connection)
                if msgLen > 0:
                    ans = recvall(connection, msgLen)

            sendHeader(connection, 4, 8)
            status =1 
            connection.send(status.to_bytes(4,'big'))

            print ('Ora mi collego al socket')
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((ipAddr, port))
            threading.Thread(target = handleNotification, args = (sock, )).start()
    except:
        print('Connection terminated')
    

def handleMonitor(connection):
    try:
        while True:
            print('ATTESA COMANDO')
            msgLen, nargs = recvHeader(connection)
            print('RICEVUTO HEADER '+str(msgLen))
            ans = recvall(connection, msgLen)
            cmd = ans.decode('utf-8')
            print('Command: '+cmd)
            if cmd != 'ServerQAction($,$,$,$,$,$,$,$,$,$,$,$)':
                continue
            msgLen, nargs = recvHeader(connection)
            ans = recvall(connection, 1)
            ipAddr = ''
            ipAddr += str(int.from_bytes(ans, 'big'))
            ans = recvall(connection, 1)
            ipAddr += '.'+str(int.from_bytes(ans, 'big'))
            ans = recvall(connection, 1)
            ipAddr += '.'+str(int.from_bytes(ans, 'big'))
            ans = recvall(connection, 1)
            ipAddr += '.'+str(int.from_bytes(ans, 'big'))
            print('Ip Addr: ', ipAddr)
            msgLen, nargs = recvHeader(connection)
            ans = recvall(connection, msgLen)
            port =  int.from_bytes(ans,'big')
            print('Port: ', str(port))
            for i in range(2,12):
                print('Reading arg  '+str(i))
                msgLen, nargs = recvHeader(connection)
                if msgLen > 0:
                    ans = recvall(connection, msgLen)

            sendHeader(connection, 4, 8)
            status =1 
            connection.send(status.to_bytes(4,'big'))

            print ('Ora mi collego al socket')
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((ipAddr, port))
            threading.Thread(target = handleNotification, args = (sock, )).start()
    except:
        print('Connection terminated')



if len(sys.argv) != 5 and len(sys.argv) != 4:
    print('usage: python dispatch_monitor.py <jDispatcher property file> <port> <commands port> [redis server]')
    sys.exit(0)
if len(sys.argv) == 4:
    red = redis.Redis(host='localhost')
else:
    red = redis.Redis(host=sys.argv[4])

port = int(sys.argv[2])
commandPort = int(sys.argv[3])
phaseDict = getPhaseDict(sys.argv[1])



thread = threading.Thread(target = manageCommands, args = (commandPort, ))
thread.start()
 
serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
print('Port: '+str(port))
host = socket.gethostname()
print('Host: ', host)
serversocket.bind((host, port))
serversocket.listen(5) # become a server socket, maximum 5 connections

while True:

    connection, address = serversocket.accept()
    print('Ricevuta Connessione', address)
    try:
        msgLen, nargs = recvHeader(connection)
        print('Ricevuto header')
        if msgLen > 0:
            recvall(connection, msgLen)
        sendHeader(connection, 0)
        monThread = threading.Thread(target = handleMonitor, args = (connection, ))
        monThread.start()
    except:
        print('Connection Terminated')
