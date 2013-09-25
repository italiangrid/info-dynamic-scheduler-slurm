# Copyright (c) Members of the EGEE Collaboration. 2004. 
# See http://www.eu-egee.org/partners/ for details on the copyright
# holders.  
#
# Licensed under the Apache License, Version 2.0 (the "License"); 
# you may not use this file except in compliance with the License. 
# You may obtain a copy of the License at 
#
#     http://www.apache.org/licenses/LICENSE-2.0 
#
# Unless required by applicable law or agreed to in writing, software 
# distributed under the License is distributed on an "AS IS" BASIS, 
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
# See the License for the specific language governing permissions and 
# limitations under the License.

import sys
import shlex
import subprocess
from threading import Thread

from SLURMInfoUtils import CommonUtils

class PolicyData:

    def __init__(self):
        self.maxWallTime = -1
        self.maxCPUTime = -1
        self.maxRunJobs = -1
        self.maxTotJobs = -1
        self.priority = 2147483647
        
    def compAndSet(self, policyData):
        self.maxWallTime = max(self.maxWallTime, policyData.maxWallTime)
        self.maxCPUTime = max(self.maxCPUTime, policyData.maxCPUTime)
        self.maxRunJobs = max(self.maxRunJobs, policyData.maxRunJobs)
        self.maxTotJobs = max(self.maxTotJobs, policyData.maxTotJobs)
        self.priority = min(self.priority, policyData.priority)

class PolicyTable:

    def __init__(self):
        self.table = dict()
        
    def __getitem__(self, kTuple):
    
        vogrp, queue = self._normTuple(kTuple)
        
        if vogrp <> None and queue <> None:
            return self.table[kTuple]
        
        if vogrp <> None or queue <> None:
            if kTuple in self.table:
                return self.table[kTuple]
                
            foundKey = False
            tmpPol = PolicyData()
            for tmpt in self.table:
                if (vogrp <> None and tmpt[0] == vogrp) or (queue <> None and tmpt[1] == queue):
                    foundKey = True
                    tmpPol.compAndSet(self.table[tmpt])
            if foundKey:
                return tmpPol
        
        raise KeyError("Missing key: %s %s" % kTuple)
        

    def __setitem__(self, kTuple, data):
        self.table[self._normTuple(kTuple)] = data
    
    def __contains__(self, kTuple):
    
        vogrp, queue = self._normTuple(kTuple)
        
        if vogrp <> None and queue <> None:
            return (vogrp, queue) in self.table
        
        if vogrp <> None:
            for tmpt in self.table:
                if tmpt[0] == vogrp:
                    return True

        if queue <> None:
            for tmpt in self.table:
                if tmpt[1] == queue:
                    return True

        return False
    
    def _normTuple(self, kTuple):
        v = kTuple[0]
        q = kTuple[1]
        if v <> None and len(v.strip()) == 0:
            v = None
        if q <> None and len(q.strip()) == 0:
            q = None
        return (v, q)



class PolicyInfoHandler(Thread):

    def __init__(self):
        Thread.__init__(self)
        self.errList = list()
        self.policyTable = PolicyTable()

    def setStream(self, stream):
        self.stream = stream
    
    def run(self):
    
        colIdx = dict()
        
        line = self.stream.readline()
        
        if line:
            idx = 0
            for colName in line.strip().split('|'):
                colIdx[colName.lower()] = idx
                idx += 1
            line = self.stream.readline()
        
        while line:
            tmpl = line.strip().split('|')
            
            account = tmpl[colIdx['account']]
            userName = tmpl[colIdx['user']]
            queue = tmpl[colIdx['partition']]
            
            policy = PolicyData()
            
            tmps = tmpl[colIdx['maxwall']]
            if tmps:
                policy.maxWallTime = CommonUtils.convertTimeLimit(tmps)
            
            tmps = tmpl[colIdx['maxcpumins']]
            if tmps:
                policy.maxCPUTime = int(tmps) * 60
            
            tmps = tmpl[colIdx['maxjobs']]
            if tmps:
                policy.maxRunJobs = int(tmps)
            
            tmps = tmpl[colIdx['maxsubmit']]
            if tmps:
                policy.maxTotJobs = int(tmps)

            tmps = tmpl[colIdx['share']]
            #
            # TODO missing priority for parent
            #
            if tmps and tmps <> 'parent':
                policy.priority = int(tmps)

            
            #
            # Let's assume the account is the vo name
            #
            vogrp = account
            
            if (vogrp, queue) in self.policyTable:
                self.policyTable[vogrp, queue] = policy
            else:
                self.policyTable[vogrp, queue].compAndSet(policy)
            
            line = self.stream.readline()






def parsePolicies():

    cmd = shlex.split('sacctmgr -P show associations')
    container = PolicyInfoHandler()
    CommonUtils.parseStream(cmd, container)
    return container




