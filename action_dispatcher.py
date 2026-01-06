#!/usr/bin/env python
import MDSplus
import redis
import threading
import traceback
import sys
import os
import time

treeDEPENDENCY_AND = 10
treeDEPENDENCY_OR = 11
treeDEPENDENCY_OF = 12
treeLOGICAL_AND = 45
treeLOGICAL_OR = 267
treeLOGICAL_OF = 229


def getDepActionNids(actionNode):
    action = actionNode.getData()
    dispatch = action.getDispatch()
    when = dispatch.getWhen()
    if isinstance(when, MDSplus.Compound):
        opcode = when.getOpcode()
        if opcode == treeDEPENDENCY_AND or opcode == treeLOGICAL_AND or opcode == treeDEPENDENCY_OR or opcode == treeLOGICAL_OR:
            if isinstance(when.getArgumentAt(0), MDSplus.TreeNode) or isinstance(when.getArgumentAt(0), MDSplus.TreePath):
                 leftSide = [when.getArgumentAt(0).getNid()]
            elif  isinstance(when.getArgumentAt(0), MDSplus.Compound):
                leftSide = getDepActionNids(when.getArgumentAt(0)) 
            else:
                leftSide = []
            if isinstance(when.getArgumentAt(1), MDSplus.TreeNode) or isinstance(when.getArgumentAt(1), MDSplus.TreePath):
                rightSide = [when.getArgumentAt(1).getNid()]
            elif  isinstance(when.getArgumentAt(1), MDSplus.Compound):
                rightSide = getDepActionNids(when.getArgumentAt(1)) 
            else:
                rightSide = []
            return leftSide + rightSide    
        if opcode ==  treeDEPENDENCY_OF or opcode == treeLOGICAL_OF:
            return [when.getArgumentAt(1).getNid()]
    if isinstance(when, MDSplus.TreeNode) or isinstance(when, MDSplus.TreePath):
        if when.getNid() != None:
            return [when.getNid()] 
    return []



class ActionDispatcher:
    # local information
    # seqActions keeps track of the sequential actions for this ident
    # seqActions Dictionary{tree+shot:{Dictionary{Phase:Dictionary{ServerClass: Dictionary{seqNum:Nid list}}}}
    #
    # depActions keeps track of the dependent actions for this ident
    # depActions Dictionary{tree+shot:{Dictionary{Phase:Dictionary{ServerClass: Nid list}}}
    #
    # dependencies Dictionary{tree+shot:{Dictionary{nid: Dependency}}
    #
    # depAffected keeps track for each action nid the list of  potentially affected action nids
    # depAffected Dictionary{tree+shot:{Dictionary{nid:list of affected nids}}
    # 
    # idents keeps track for every action nid the associated ident (server Class)
    # idents  Dictionary{tree+shot:{Dictionary{Nid:ident}}
    # 
    # timeouts keeps track for every action nid the associated timeout (None is not defined)
    # timeouts Dictionary{tree+shot:{Dictionary{Nid: Timeout}}
    #
    # completionEvent keeps track for every action nid for this action server the possible completion event
    # completionEvent Dictionary{tree+shot:{Dictionary{nid: event name}}
    # 
    # actionDispatchStatus keeps track of the current dispatching status of actions (NOT_DISPATCHED, DISPATCHED, DONE)
    # actionDispatchStatus Dictionary{tree+shot:{Dictionary{nid: status}} 
    # 
    # identList keeps the list of all server classes (idents) handled by this dispatcher
 

    def __init__(self, red):
        print('starting __init__')
        self.seqActions = {}
        self.depActions = {}
        self.dependencies = {}
        self.depAffected = {}
        self.idents = {}
        self.timeouts = {}
        self.completionEvent = {}
        self.actionDispatchStatus = {}
        self.identList = []
        print('pubsub')
        self.cmdPubsub = red.pubsub()
        print('subscribe')
        self.cmdPubsub.subscribe('ACTION_DISPATCHER_COMMANDS')
        print('pubsub')
        self.updPubsub = red.pubsub()
        print('subscribe')
        self.updPubsub.subscribe('ACTION_DISPATCHER_PUBSUB')
        print('subscribed')
        self.NOT_DISPATCHED = 1
        self.DISPATCHED = 2
        self.DOING = 3
        self.DONE = 4
        self.updateMutex = threading.Lock()
        self.updateEvent = threading.Event()
        self.red = red
        self.doing = False
        self.aborted = False
        self.pendingSeqActions = {}
        self.pendingDepActions = {}
        print('object initialized')


    def printTables(self):
        print("******Sequential Actions")
        print(self.seqActions)
        print("\n******Dependent Actions")
        print(self.depActions)
        print("\n******Dependency Affected")
        print(self.depAffected)
        print("\n******Completion Events")
        print(self.completionEvent)
        print("\n******Idents")
        print(self.idents)
        print("\n******Timeouts")
        print(self.timeouts)
        print("\n******Dependencies")
        print(self.dependencies)
        print("\n******Dispatch Status")
        print( self.actionDispatchStatus)




    def buildTables(self, tree):
        print("BUILD TABLES "+ tree.name+'  '+str(tree.shot))
        dd = tree.getNodeWild('***', 'ACTION')
        treeShot = tree.name+str(tree.shot)
        self.seqActions[treeShot] = {}
        self.depActions[treeShot] = {}
        self.depAffected[treeShot] = {}
        self.completionEvent[treeShot] = {}
        self.idents[treeShot] = {}
        self.timeouts[treeShot] = {}
        self.dependencies[treeShot] = {}
        self.actionDispatchStatus[treeShot] = {}
        self.identList = []
        for idx in range(len(dd)):
            d = dd[idx]
            print(d.getPath())
            try:
                disp = d.getData().getDispatch()
                when = disp.getWhen()
                phase = disp.getPhase().data().upper()
                ident = disp.getIdent().data()
                actNid = d.getNid()
            except:
                print('Error reading action '+d.getPath())
                continue
            if not ident in self.identList:
                self.identList.append(ident)
            if d.isOn():
#                if idx == 0:
#                    self.red.publish('DISPATCH_MONITOR_PUBSUB', 'BUILD_BEGIN+'+ tree.name+'+'+str(tree.shot)+'+'+phase+'+'+str(tree.getNode(d.getFullPath()).getNid())+'+1+'+ident+'+'+d.getFullPath())
#                elif idx == len(dd) - 1:
#                    self.red.publish('DISPATCH_MONITOR_PUBSUB', 'BUILD_END+'+ tree.name+'+'+str(tree.shot)+'+'+phase+'+'+str(tree.getNode(d.getFullPath()).getNid())+'+1+'+ident+'+'+d.getFullPath())
#                else:
#                    self.red.publish('DISPATCH_MONITOR_PUBSUB', 'BUILD+'+ tree.name+'+'+str(tree.shot)+'+'+phase+'+'+str(tree.getNode(d.getFullPath()).getNid())+'+1+'+ident+'+'+d.getFullPath())
                try:
                    try:
                        timeout = int(d.getTimeout().data())
                    except:
                        timeout = 0
                    if not phase in self.seqActions[treeShot].keys():
                        self.seqActions[treeShot][phase] = {}
                    if not ident in self.seqActions[treeShot][phase].keys(): 
                        self.seqActions[treeShot][phase][ident] = {} 
                    if not phase in self.depActions[treeShot].keys():
                        self.depActions[treeShot][phase] = {}
                    if not ident in self.depActions[treeShot][phase].keys(): 
                        self.depActions[treeShot][phase][ident] = []
                    if isinstance(when, MDSplus.Scalar):
                        seqNum = int(when.data())
                        if not seqNum in self.seqActions[treeShot][phase][ident].keys():
                                self.seqActions[treeShot][phase][ident][seqNum] = []
                        self.seqActions[treeShot][phase][ident][seqNum].append(actNid)
                    else: #dependent action
                        self.depActions[treeShot][phase][ident].append(actNid)
                    # record completion event if any
                    completionName = disp.getCompletion().getString()
                    if completionName != '':
                        self.completionEvent[treeShot][actNid] = completionName
                    self.idents[treeShot][actNid] = ident
                    self.timeouts[treeShot][actNid] = timeout
                    if not isinstance(when, MDSplus.Scalar): #if it is a dependent action
                        self.dependencies[treeShot][d.getNid()] = when
                    depNids = getDepActionNids(d) #get all the actions included in this dependency
                    for depNid in depNids:
                        if not depNid in self.depAffected[treeShot].keys():
                            self.depAffected[treeShot][depNid] = []
                        self.depAffected[treeShot][depNid].append(d.getNid())

                    self.actionDispatchStatus[treeShot][actNid] = self.NOT_DISPATCHED
                    self.red.hset('ACTION_INFO:'+tree.name+':'+str(tree.shot)+':'+ident, tree.getNode(actNid).getFullPath(), 'NOT_DISPATCHED')
                    self.red.hset('ACTION_SERVER_INFO:'+tree.name+':'+str(tree.shot), tree.getNode(actNid).getFullPath(), ident)
                    self.red.hset('ACTION_PHASE_INFO:'+tree.name+':'+str(tree.shot), tree.getNode(actNid).getFullPath(), phase)
                    self.red.hset('ACTION_STATUS:'+tree.name+':'+str(tree.shot), tree.getNode(actNid).getFullPath(), 'none')
                except Exception as e:
                    print('Error collecting action ' + d.getPath()+ ': '+str(e))
            else: #d is off
 #               if idx == 0:
 #                   self.red.publish('DISPATCH_MONITOR_PUBSUB', 'BUILD_BEGIN+'+ tree.name+'+'+str(tree.shot)+'+'+phase+'+'+str(tree.getNode(d.getFullPath()).getNid())+'+0+'+ident+'+'+d.getFullPath())
 #               elif idx == len(dd) - 1:
 #                   self.red.publish('DISPATCH_MONITOR_PUBSUB', 'BUILD_END+'+ tree.name+'+'+str(tree.shot)+'+'+phase+'+'+str(tree.getNode(d.getFullPath()).getNid())+'+0+'+ident+'+'+d.getFullPath())
 #               else:
 #                   self.red.publish('DISPATCH_MONITOR_PUBSUB', 'BUILD+'+ tree.name+'+'+str(tree.shot)+'+'+phase+'+'+str(tree.getNode(d.getFullPath()).getNid())+'+0+'+ident+'+'+d.getFullPath())
                self.idents[treeShot][actNid] = ident  #Record in any case the action's server
                self.red.hset('ACTION_STATUS:'+tree.name+':'+str(tree.shot), d.getFullPath(), 'none')
                self.red.hset('ACTION_SERVER_INFO:'+tree.name+':'+str(tree.shot),  d.getFullPath(), ident)
                self.red.hset('ACTION_INFO:'+tree.name+':'+str(tree.shot)+':'+ident, tree.getNode(actNid).getFullPath(), 'OFF')

        
        self.printTables()

    def handleAbort(self):
        self.aborted = True
        self.updateEvent.set()


    def doSequence(self, tree, phase, startSeqNumber, endSeqNumber):
        if self.doing:
            print("Sequence already in progress")
            return
        self.doing = True
#        self.red.publish('DISPATCH_MONITOR_PUBSUB', 'START_SEQUENCE+'+ tree.name+'+'+str(tree.shot)+'+'+phase)
        treeShot = tree.name+str(tree.shot)
        if not treeShot in self.seqActions.keys():
            print('Dispatch Table missing')
            return
        self.currSeqNumbers = {}
        self.pendingSeqActions = {}
        self.pendingDepActions = {}
        self.endSeqNumber = endSeqNumber
        for ident in self.seqActions[treeShot][phase].keys():
            self.pendingSeqActions[ident] = []
            self.currSeqNumbers[ident] = startSeqNumber - 1

        self.updateEvent.clear()
        self.performSequenceStep(tree, phase)
        while not self.allSeqTerminated:
            self.updateEvent.wait()
            self.updateEvent.clear()
            if self.aborted:
                print('Sequence aborted')
                self.doing = False
                self.aborted = False
                return
            self.performSequenceStep(tree, phase)
        self.doing = False
#        self.red.publish('DISPATCH_MONITOR_PUBSUB', 'END_SEQUENCE+'+ tree.name+'+'+str(tree.shot)+'+'+phase)
        print('DoSequence terminated')

    def serverExists(self, ident):
        for id in range(50): #no more than 50 servers per class assumed....
            serverStatus = red.hget('ACTION_SERVER_ACTIVE:'+ident, str(id)) 
            if serverStatus == b'ON':
                return True
        return False 

    
    def performSequenceStep(self, tree, phase):
        treeShot = tree.name+str(tree.shot)
        self.allSeqTerminated = True
        for ident in self.seqActions[treeShot][phase].keys():
            serverExists = self.serverExists(ident)
            if len(self.pendingSeqActions[ident]) == 0:
                self.currSeqNumbers[ident] += 1 
                while self.currSeqNumbers[ident] <= self.endSeqNumber and not self.currSeqNumbers[ident] in self.seqActions[treeShot][phase][ident].keys():
                    self.currSeqNumbers[ident] += 1 
                if self.currSeqNumbers[ident] <= self.endSeqNumber:
                    if serverExists:
                        self.allSeqTerminated = False
                    for actNid in self.seqActions[treeShot][phase][ident][self.currSeqNumbers[ident]]:
                        fullPath = tree.getNode(actNid).getFullPath()
                        if serverExists:
                            self.pendingSeqActions[ident].append(actNid)
                            self.red.lpush('ACTION_SERVER_TODO:'+ident, 
                                tree.name+'+'+str(tree.shot)+'+'+tree.getNode(actNid).getFullPath()+'+'+str(actNid)+'+'+str(self.timeouts[treeShot][actNid]))
                            print('Dispatching action '+fullPath+'   Tree: '+tree.name+'  Shot: '+str(tree.shot))
                            self.actionDispatchStatus[treeShot][actNid] = self.DISPATCHED
                            self.red.hset('ACTION_INFO:'+tree.name+':'+str(tree.shot)+':'+ident, fullPath, 'DISPATCHED')
                            self.red.hset('ACTION_STATUS:'+tree.name+':'+str(tree.shot), fullPath, 'None') 
#                            self.red.publish('DISPATCH_MONITOR_PUBSUB', 'DISPATCHED+'+ tree.name+'+'+str(tree.shot)+'+'+phase+'+'+ident+'+'+fullPath+'+'+str(actNid))
                        else:
                            print('SERVER MISSING for '+fullPath)
                            #self.red.hset('ACTION_INFO:'+tree.name+':'+str(tree.shot)+':'+ident, fullPath, 'DONE')
                            self.red.hset('ACTION_INFO:'+tree.name+':'+str(tree.shot)+':'+ident, fullPath, 'SERVER_OFF')
                            self.red.hset('ACTION_STATUS:'+tree.name+':'+str(tree.shot), fullPath, 'NotExecuted')
#                            self.red.publish('DISPATCH_MONITOR_PUBSUB', 'DISPATCHED+'+ tree.name+'+'+str(tree.shot)+'+'+phase+'+'+ident+'+'+fullPath+'+'+str(actNid))
#                            self.red.publish('DISPATCH_MONITOR_PUBSUB', 'DOING+'+ tree.name+'+'+str(tree.shot)+'+'+ident+'+0+'+fullPath+'+'+str(actNid))
#                            self.red.publish('DISPATCH_MONITOR_PUBSUB', 'DONE+'+ tree.name+'+'+str(tree.shot)+'+'+ident+'+0+'+fullPath+'+'+str(actNid)+'+0')

                    if serverExists:        
                        self.red.publish('ACTION_SERVER_PUBSUB:'+ident, 'DO')
            else: #There are still pending actions
                self.allSeqTerminated = False
            

    def doPhase(self, tree, phase):
        self.red.hset('DISPATCH_INFO', 'CURR_PHASE', phase)
        self.currPhase = phase
        treeShot = tree.name+str(tree.shot)
        if not treeShot in self.seqActions.keys():
            print('Dispatch Table missing')
            return
#        self.red.publish('DISPATCH_MONITOR_PUBSUB', 'START_PHASE+'+ tree.name+'+'+str(tree.shot)+'+'+phase)
        try:
            seqIdents = self.seqActions[treeShot][phase].keys()
            minSeqNumber = sys.maxsize
            maxSeqNumber = 0
            for seqIdent in seqIdents:
                for seqNum in self.seqActions[treeShot][phase][seqIdent]:
                    if seqNum > maxSeqNumber:
                        maxSeqNumber = seqNum
                    if seqNum < minSeqNumber:
                        minSeqNumber = seqNum
            self.doSequence(tree, phase, minSeqNumber, maxSeqNumber)  
#            self.red.publish('DISPATCH_MONITOR_PUBSUB', 'END_PHASE+'+ tree.name+'+'+str(tree.shot)+'+'+self.currPhase)
        except:
#            self.red.publish('DISPATCH_MONITOR_PUBSUB', 'END_PHASE+'+ tree.name+'+'+str(tree.shot)+'+'+self.currPhase)
            print('Either phase('+phase+'), tree ('+tree.name+') or shot('+str(tree.shot)+') are missing in dispatch tables')      

    def handleCommands(self):
        while True:
            print('Waiting Command....')
            message = self.cmdPubsub.get_message(timeout=100)
            if message == None:
                continue
            if not 'data' in message.keys() or not isinstance(message['data'], bytes):
                continue
            msg = message['data'].decode('utf8')
            print('Received command: ', msg)
            if msg.upper() == 'QUIT':
                os._exit(0)
            elif msg.upper() == 'ABORT':
                self.handleAbort()
            elif msg.upper()[:12] == 'CREATE_PULSE':
                parts = msg.split(':')
                if len(parts) != 3:
                    print('INVALID COMMAND: '+msg)
                    continue
                treeName = parts[1]
                shot = int(parts[2])
                tree = MDSplus.Tree(treeName, -1)
                tree.createPulse(shot)
                self.red.hset('DISPATCH_INFO', 'CURR_TREE', treeName)
                self.red.hset('DISPATCH_INFO', 'CURR_SHOT', shot)
            elif msg.upper()[:12] == 'BUILD_TABLES':
                parts = msg.split(':')
                if len(parts) != 3:
                    print('Invalid command: ', msg)
                    continue
                try:
                    #until next build  table self.tree is the current tree. Used by Watchdog
                    self.tree = MDSplus.Tree(parts[1], int(parts[2]))
                except:
                    print('Cannot open tree '+parts[1] + '  shot '+parts[2])
                    continue
                self.buildTables(self.tree)
            elif msg.upper()[:8] == 'DO_PHASE':
                parts = msg.split(':')
                if len(parts) != 4:              
                    print('Invalid command: ', msg)
                    continue
                try:
                    tree = MDSplus.Tree(parts[1], int(parts[2]))
                except:
                    print('Cannot open tree '+parts[1] + '  shot '+parts[2])
                    continue
#report current phase for dispatch monitor
                self.red.hset('CURRENT_PHASE', parts[1], parts[3])

                thread = threading.Thread(target = self.doPhase, args = (tree, parts[3].upper(), ))
                thread.start()
            elif msg.upper()[:11] == 'DO_SEQUENCE':
                parts = msg.split(':')
                if len(parts) != 6:              
                    print('Invalid command: ', msg)
                    continue
                try:
                    tree = MDSplus.Tree(parts[1], int(parts[2]))
                except:
                    print('Cannot open tree '+parts[1] + '  shot '+parts[2])
                    continue
                self.doSequence(tree, parts[3], int(parts[4]), int(parts[5]))
            else:
                print('Unknown command: '+msg)


    def handleNotifications(self):
        while True:
            message = self.updPubsub.get_message(timeout=100)
            if message == None or not 'data' in message.keys() or not isinstance(message['data'], bytes):
                continue
            msg = message['data'].decode('utf8')
            parts = msg.split('+')
            if len(parts) < 5:
                print('Invalid Update Command: '+msg)
                continue
            treeName = parts[0]
            shot = parts[1]
            treeShot = parts[0] + parts[1]
            try:
                tree = MDSplus.Tree(treeName, int(shot))
            except:
                print('Cannot open tree '+treeName+ ' shot '+shot)
                continue
            ident = parts[2]
            try:
                actionNid = tree.getNode(parts[3]).getNid()
            except:
                print('Cannot find node '+parts[3])
                continue
            path = parts[3]
            print('Action '+parts[3]+ '   terminated. Status:  '+ parts[4])
            self.actionDispatchStatus[treeShot][actionNid] = self.DONE
            self.red.hset('ACTION_STATUS:'+treeName+':'+str(shot), parts[3], parts[4]) 
            if len(parts) >= 4:
                self.red.hset('ACTION_LOG:'+treeName+':'+str(shot), parts[3], msg[len(parts[0])+len(parts[1])+len(parts[2])+len(parts[3])+len(parts[4])+5:])
##for debug
#                print(msg[len(parts[0])+len(parts[1])+len(parts[2])+len(parts[3])+len(parts[4])+5:])

#handle sequence

            self.updateMutex.acquire()
            if not ident in self.pendingSeqActions.keys():
                print('Internal error: unextected ident: '+ident)
                self.updateMutex.release()
                continue
            if actionNid in self.pendingSeqActions[ident]:
                self.pendingSeqActions[ident].remove(actionNid)
            if len(self.pendingSeqActions[ident]) == 0:
                self.updateEvent.set()

            if ident in  self.pendingDepActions.keys() and actionNid in self.pendingDepActions[ident]:
                self.pendingDepActions[ident].remove(actionNid)

#handle dependencies
            if actionNid in self.depAffected[treeShot].keys():
                for depNid in self.depAffected[treeShot][actionNid]:
                    if self.checkDispatch(tree, depNid):
                        ident = self.idents[treeShot][depNid]
                        serverExists = self.serverExists(ident)
                        if serverExists:
                            self.red.lpush('ACTION_SERVER_TODO:'+ident, 
                                treeName+'+'+str(shot)+'+'+tree.getNode(depNid).getFullPath()+'+'+str(depNid)+'+'+str(self.timeouts[treeShot][depNid]))
                            print('Dispatching action '+tree.getNode(depNid).getFullPath()+'   Tree: '+tree.name+'  Shot: '+str(tree.shot))
                            self.red.hset('ACTION_INFO:'+treeName+':'+str(shot)+':'+ident, tree.getNode(depNid).getFullPath(), 'DISPATCHED')
                            self.red.hset('ACTION_STATUS:'+tree.name+':'+str(tree.shot), tree.getNode(depNid).getFullPath(), 'none')
                            if not ident in self.pendingDepActions.keys():
                                self.pendingDepActions[ident] = []
                            self.pendingDepActions[ident].append(depNid)
                            #in ogni caso
                            self.red.publish('ACTION_SERVER_PUBSUB:'+ident, 'DO')
                        else:       
                            print('SERVER MISSING for '+tree.getNode(depNid).getFullPath())
                            self.red.hset('ACTION_INFO:'+tree.name+':'+str(tree.shot)+':'+ident, tree.getNode(depNid).getFullPath(), 'SERVER_OFF')
                            self.red.hset('ACTION_STATUS:'+tree.name+':'+str(tree.shot), tree.getNode(depNid).getFullPath(), 'NotExecuted')


            self.updateMutex.release()
            if self.allSeqTerminated:
                allDepTerminated = True
                for ident in self.pendingDepActions.keys():
                    if len( self.pendingDepActions[ident]) > 0:
                        allDepTerminated = False
                if allDepTerminated:
                    print('Phase '+self.currPhase+ ' terminated')
#                    self.red.publish('DISPATCH_MONITOR_PUBSUB', 'END_PHASE+'+ tree.name+'+'+str(tree.shot)+'+'+self.currPhase)


#remove pending operations for a dead server
    def removeDeadPending(self, tree, ident, id):
        self.updateMutex.acquire()
        if ident in self.pendingSeqActions.keys():
            for actionNid in self.pendingSeqActions[ident]:
                fullPath = tree.getNode(actionNid).getFullPath()
                statusInfo = red.hget('ACTION_INFO:'+tree.name+':'+str(tree.shot)+':'+ident, fullPath)
                if statusInfo == None:
                    print('********************\nInternal error: Missing Action Info for '+fullPath+'\n********************')
                    break
                statusInfos = statusInfo.decode('utf-8').split()
    #            if statusInfos[0] == 'DISPATCHED' or (statusInfos[0] == 'DOING' and statusInfo[1] == ident):
                if statusInfos[0] == 'DISPATCHED' or statusInfos[0] == 'DOING':
                    print('Removing action due to server crash: ', fullPath)
                    self.pendingSeqActions[ident].remove(actionNid)
                    self.red.hset('ACTION_INFO:'+tree.name+':'+str(tree.shot)+':'+ident, fullPath, 'DONE')
                    self.red.hset('ACTION_STATUS:'+tree.name+':'+str(tree.shot), fullPath, 'ServerCrashed')
#                    self.red.publish('DISPATCH_MONITOR_PUBSUB', 'DONE+'+ tree.name+'+'+str(tree.shot)+'+'+ident+'+0+'+fullPath+'+'+str(actionNid)+'+0')
                    if len(self.pendingSeqActions[ident]) == 0:
                        self.updateEvent.set()
    #same for pending dependent  actions
        if ident in self.pendingDepActions.keys(): 
            for actionNid in self.pendingDepActions[ident]:
                fullPath = tree.getNode(actionNid).getFullPath()
                statusInfo = red.hget('ACTION_INFO:'+tree.name+':'+str(tree.shot)+':'+ident, fullPath)
                if statusInfo == None:
                    print('Internal error: Missing Action Info for '+fullPath)
                    break
                statusInfos = statusInfo.decode('utf-8').split()
                #if statusInfos[0] == 'DOING' and statusInfo[1] == ident:
                if statusInfos[0] == 'DOING':
                    print('TROVATA AZIONE SERVER MORTO!!!!!!!!!!!!!!!!!!!', fullPath)
                    self.pendingDepActions[ident].remove(actionNid)
                    self.red.hset('ACTION_INFO:'+tree.name+':'+str(tree.shot)+':'+ident, fullPath, 'DONE')
                    self.red.hset('ACTION_STATUS:'+tree.name+':'+str(tree.shot), fullPath, 'ServerCrashed')
#                    self.red.publish('DISPATCH_MONITOR_PUBSUB', 'DONE+'+ tree.name+'+'+str(tree.shot)+'+'+ident+'+0+'+fullPath+'+'+str(actionNid)+'+0')
                    if len(self.pendingDepActions[ident]) == 0:
                        self.updateEvent.set()
        self.updateMutex.release()


    def getServerIds(self, ident):
        ids = []
        for id in range(50): #no more than 5 servers per class assumed....
            serverStatus = red.hget('ACTION_SERVER_ACTIVE:'+ident, str(id)) 
            if serverStatus != None:
                ids.append(id)
        return ids 


    def serverWatchdog(self):
        wasAlive = {}
        heartbeats = {}
        while True:
            idents = self.identList[:]
            for ident in idents:
                ids = self.getServerIds(ident)
                if len(ids) == 0:
                    ids = [1] #Handle the case the server did not start and did not register itself 
                for id in ids:
                    currHeartbeat = self.red.hget('ACTION_SERVER_HEARTBEAT:'+ident, str(id)) 
                    if currHeartbeat == None: #Server never started
                        red.hset('ACTION_SERVER_ACTIVE:'+ident, str(id), 'OFF')  
                        if not (ident+':'+str(id)) in wasAlive.keys() or wasAlive[ident+':'+str(id)]:
                            self.removeDeadPending(self.tree, ident, id)
                        wasAlive[ident+':'+str(id)] = False
                    else:
                        if not ident in heartbeats.keys():
                            heartbeats[ident] = {}
                        heartbeats[ident][id] = int(currHeartbeat)
                        self.red.publish('ACTION_SERVER_PUBSUB:'+ident, 'HEARTBEAT+'+str(id))
            time.sleep(5)
            for ident in idents:
                ids = self.getServerIds(ident)
                for id in ids:
                    currHeartbeat = self.red.hget('ACTION_SERVER_HEARTBEAT:'+ident, str(id))   
                    if currHeartbeat == None: #Server never started
                        red.hset('ACTION_SERVER_ACTIVE:'+ident, str(id), 'OFF')  
                        if not (ident+':'+str(id)) in wasAlive.keys() or wasAlive[ident+':'+str(id)]:
                            self.removeDeadPending(self.tree, ident, id)
                        wasAlive[ident+':'+str(id)] = False
                    else:
                        if heartbeats[ident][id] != int(currHeartbeat) - 1:  #server died
                            red.hset('ACTION_SERVER_ACTIVE:'+ident, str(id), 'OFF')  
                            if not (ident+':'+str(id)) in wasAlive.keys() or wasAlive[ident+':'+str(id)]:
                                print('Watchdog Failed for server class'+ident+' id '+str(id)+': server not responding')
                                self.removeDeadPending(self.tree, ident, id)
                            wasAlive[ident+':'+str(id)] = False
                        else:
                            wasAlive[ident+':'+str(id)] = True
                            red.hset('ACTION_SERVER_ACTIVE:'+ident, str(id), 'ON')  
  







# return True if the dispatching condition is satisfied

    def checkDispatch(self, tree, actionNid):
        action = tree.getNode(actionNid).getData()
        dispatch = action.getDispatch()
        when = dispatch.getWhen()
        done = self.checkDone(when, tree)
        return done
    
    def checkDone(self, when, tree):
        treeShot = tree.name+str(tree.shot)
        if isinstance(when, MDSplus.TreeNode):
            nid = when.getNid()
            if not nid in self.actionDispatchStatus[treeShot].keys():
                print('Internal error: nid not found: '+tree.getNode(nid).getPath())
                return False
            return self.actionDispatchStatus[treeShot][nid] == self.DONE
        if isinstance(when, MDSplus.Compound):
            opcode = when.getOpcode()
            if opcode == treeDEPENDENCY_AND or opcode ==  treeLOGICAL_AND:
                return self.checkDone(when.getArgumentAt(0), tree) and self.checkDone(when.getArgumentAt(1), tree)
            if opcode ==  treeDEPENDENCY_OR or opcode ==  treeLOGICAL_OR:
                return self.checkDone(when.getArgumentAt(0), tree) or self.checkDone(when.getArgumentAt(1), tree)
        print('Invalid when condition: '+when)
        return False
                                
#####End Class ActionDispatcher
from threading import Thread
from time import sleep

def manageNotifications(actDisp):
    actDisp.handleNotifications()

def manageWatchdog(actDisp):
    actDisp.serverWatchdog()

import argparse

parser = argparse.ArgumentParser()

parser.add_argument(
    "host",
    nargs="?",
    help="Redis host"
)

parser.add_argument(
    "password",
    nargs="?",
    help="Redis password (requires host)"
)

args = parser.parse_args()

# Optional sanity check
if args.password and not args.host:
    parser.error("password requires host")

host = args.host or 'localhost'
password = args.password

print(args.host, args.password)

if password:
    red = redis.StrictRedis(host=host, password=password)
else:
    red = redis.Redis(host=host)

act = ActionDispatcher(red)
thread = Thread(target = manageNotifications, args = (act, ))
thread.start()
threadWatch = Thread(target = manageWatchdog, args = (act, ))
threadWatch.start()
act.handleCommands()




                        





                





