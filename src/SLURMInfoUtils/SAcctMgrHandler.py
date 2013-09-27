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
import pwd
import grp
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

VOGRP=0
QUEUE=1
class PolicyTable:

    def __init__(self):
        self.table = dict()
        
    def __getitem__(self, kTuple):
    
        nTuple = self._normTuple(kTuple)
        
        if nTuple[VOGRP] <> None and nTuple[QUEUE] <> None:
            return self.table[nTuple]
        
        if nTuple[VOGRP] <> None or nTuple[QUEUE] <> None:
            if nTuple in self.table:
                return self.table[nTuple]
                
            foundKey = False
            tmpPol = PolicyData()
            for tmpt in self.table:
                if (nTuple[VOGRP] <> None and tmpt[0] == nTuple[VOGRP]) \
                    or (nTuple[QUEUE] <> None and tmpt[1] == nTuple[QUEUE]):
                    foundKey = True
                    tmpPol.compAndSet(self.table[tmpt])
            if foundKey:
                return tmpPol
        
        raise KeyError("Missing key: %s %s" % kTuple)
        

    def __setitem__(self, kTuple, data):
        self.table[self._normTuple(kTuple)] = data
    
    def __contains__(self, kTuple):
    
        nTuple = self._normTuple(kTuple)
        
        if nTuple[VOGRP] <> None and nTuple[QUEUE] <> None:
            return nTuple in self.table
        
        if nTuple[VOGRP] <> None:
            for tmpt in self.table:
                if tmpt[VOGRP] == nTuple[VOGRP]:
                    return True

        if nTuple[QUEUE] <> None:
            for tmpt in self.table:
                if tmpt[QUEUE] == nTuple[QUEUE]:
                    return True

        return False
    
    def _normTuple(self, kTuple):
        v = kTuple[VOGRP]
        q = kTuple[QUEUE]
        if v <> None and len(v.strip()) == 0:
            v = None
        if q <> None and len(q.strip()) == 0:
            q = None
        return (v, q)



class PolicyInfoHandler(Thread):

    def __init__(self, vomap, clustername):
        Thread.__init__(self)
        self.errList = list()
        self.policyTable = PolicyTable()
        self.vomap = vomap
        self.cluster = clustername

    def setStream(self, stream):
        self.stream = stream
        
    def getVOForUser(self, user):
        try:
        
            grpgid = pwd.getpwnam(user)[3]
            grpname = grp.getgrgid(grpgid)[0]
            if grpname in self.vomap:
                return self.vomap[grpname]
            return grpname
            
        except:
            return None
    
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
            
            try:
                clustername = tmpl[colIdx['cluster']]
                account = tmpl[colIdx['account']]
                userName = tmpl[colIdx['user']]
                queue = tmpl[colIdx['partition']]
            
                if clustername <> self.cluster:
                    continue
                
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

                vogrp = self.getVOForUser(userName)
                if not vogrp:
                    continue
            
                if (vogrp, queue) in self.policyTable:
                    self.policyTable[vogrp, queue].compAndSet(policy)
                else:
                    self.policyTable[vogrp, queue] = policy
                
            finally:
                line = self.stream.readline()






def parsePolicies(**argdict):

    cmd = shlex.split('sacctmgr -P show associations')
    
    if 'vomap' in argdict:
        vomap = argdict['vomap']
    else:
        vomap = dict()
    
    if 'cluster' in argdict:
        clustername = argdict['cluster']
    else:
        clustername = None
    
    container = PolicyInfoHandler(vomap, clustername)
    CommonUtils.parseStream(cmd, container)
    return container




