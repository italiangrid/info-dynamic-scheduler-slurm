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
import re
import time
import shlex
import subprocess
from threading import Thread

import CommonUtils

class NodesInfoHandler(Thread):

    def __init__(self):
        Thread.__init__(self)
        self.errList = list()
        self.ncpu = 0
        self.nfree = 0
        self.gpuTable = dict()
        self.stateRegex = re.compile('State=(IDLE|COMPLETING|ALLOCATED[+]?|DRAINING|MIXED)')
        self.tcpuRegex = re.compile('CPUTot=([0-9]+)')
        self.acpuRegex = re.compile('CPUAlloc=([0-9]+)')
        self.nodesRegex = re.compile('NodeHostName=([^\s]+)')
        self.GresRegex = re.compile('Gres=([^\s]+)')
    
    def setStream(self, stream):
        self.stream = stream
    
    
    def run(self):
        line = self.stream.readline()
        
        while line:

            try:
                parsed = self.stateRegex.search(line)
                if not parsed:
                    continue
                nodeState = parsed.group(1)
                
                parsed = self.tcpuRegex.search(line)
                if not parsed:
                    continue
                tcpu = int(parsed.group(1))
            
                parsed = self.acpuRegex.search(line)
                if not parsed:
                    continue
                acpu = int(parsed.group(1))

                self.ncpu += tcpu
                if nodeState <> "DRAINING":
                    self.nfree += (tcpu - acpu)

                parsed = self.nodesRegex.search(line)
                if not parsed:
                    continue
                nodeName = parsed.group(1)
                self.gpuTable[nodeName] = 0
                
                parsed = self.GresRegex.search(line)
                if not parsed:
                    continue
                for tok1 in parsed.group(1).split(','):
                    tmpt = tok1.split(':')
                    if len(tmpt) and tmpt[0] == 'gpu':
                        self.gpuTable[nodeName] = int(tmpt[-1]) if len(tmpt) > 1 else 1

            finally:
                line = self.stream.readline()


def parseNodesInfo(filename=None):
    if filename:
        cmd = shlex.split('cat ' + filename)
    else:
        cmd = shlex.split('scontrol -o show nodes')
    
    container = NodesInfoHandler()
    CommonUtils.parseStream(cmd, container)
    return container


class JobInfoHandler(Thread):

    def __init__(self, container):
        Thread.__init__(self)
        self.errList = list()
        self.jobTables = container
        
        self.jidRegex = re.compile('JobId=([^\s]+)')
        self.nameRegex = re.compile('Name=([^\s]+)')
        self.uidRegex = re.compile('UserId=([^(]+)')
        self.gidRegex = re.compile('GroupId=([^(]+)')
        self.jstateRegex = re.compile('JobState=(RUNNING|PENDING)')
        self.tlimitRegex = re.compile('TimeLimit=(UNLIMITED|[0-9:-]+)')
        self.subtimeRegex = re.compile('SubmitTime=([^\s]+)')
        self.sttimeRegex = re.compile('StartTime=([^\s]+)')
        self.partRegex = re.compile('Partition=([^\s]+)')
        self.ncpuRegex = re.compile('NumCPUs=([0-9]+)')

    def setStream(self, stream):
        self.stream = stream
    
    
    def convertTime(self, tstr):
        if tstr == 'Unknown':
            return 0
        return int(time.mktime(time.strptime(tstr, '%Y-%m-%dT%H:%M:%S')) + time.timezone)
    
    
    def run(self):
        line = self.stream.readline()
        
        while line:
        
            try:
                jTable = dict()
            
                parsed = self.jstateRegex.search(line)
                if not parsed:
                    continue
                if parsed.group(1) == "RUNNING":
                    jTable['state'] = 'running'
                else:
                    jTable['state'] = 'queued'
                
                parsed = self.jidRegex.search(line)
                if not parsed:
                    continue
                jTable['jobid'] = parsed.group(1)
                
                parsed = self.nameRegex.search(line)
                if not parsed:
                    continue
                jTable['name'] = parsed.group(1)
                
                parsed = self.uidRegex.search(line)
                if not parsed:
                    continue
                jTable['user'] = parsed.group(1)
                
                parsed = self.gidRegex.search(line)
                if not parsed:
                    continue
                jTable['group'] = parsed.group(1)
                
                parsed = self.partRegex.search(line)
                if not parsed:
                    continue
                jTable['queue'] = parsed.group(1)
                
                parsed = self.ncpuRegex.search(line)
                if not parsed:
                    continue
                jTable['cpucount'] = int(parsed.group(1))
                
                parsed = self.tlimitRegex.search(line)
                if parsed:
                    tmpLimit = parsed.group(1)
                    if tmpLimit <> 'UNLIMITED':
                        jTable['maxwalltime'] = CommonUtils.convertTimeLimit(tmpLimit)
                
                parsed = self.subtimeRegex.search(line)
                if not parsed:
                    continue
                else:
                    jTable['qtime'] = self.convertTime(parsed.group(1))
                
                parsed = self.sttimeRegex.search(line)
                if parsed:
                    jTable['start'] = self.convertTime(parsed.group(1))
                
                self.jobTables.append(jTable)
                
            finally:
                line = self.stream.readline()


def parseJobInfo(outWriter, filename=None):
    if filename:
        cmd = shlex.split('cat ' + filename)
    else:
        cmd = shlex.split('scontrol -o show jobs')
    
    container = JobInfoHandler(outWriter)
    CommonUtils.parseStream(cmd, container)


class ConfigInfoHandler(Thread):

    def __init__(self):
        Thread.__init__(self)
        self.errList = list()
        self.pRegex = re.compile('^\s*([^=\s]+)\s*=(.+)$')

        self.version = ''
        self.selectType = 'select/linear'
        self.selectParams = ''
        self.slotType = 'NODE'
        self.maxJobCount = 10000
        self.acctEnabled = False
        self.clustername = ''
        self.vSizeFactor = 0
        
    def setStream(self, stream):
        self.stream = stream

    def run(self):
        line = self.stream.readline()
        
        while line:
        
            parsed = self.pRegex.match(line)
            if parsed:
                key = parsed.group(1).lower()
                value = parsed.group(2).strip(' \n\t"')
                
                if key == 'slurm_version':
                    self.version = value
                
                if key == 'selecttype':
                    self.selectType = value
                
                if key == 'selecttypeparameters':
                    self.selectParams = value
                    
                if key == 'maxjobcount':
                    self.maxJobCount = int(value)
                
                if key == 'accountingstoragetype':
                    self.acctEnabled = value == 'accounting_storage/slurmdbd'
            
                if key == 'clustername':
                    self.clustername = value
            
                if key == 'vsizefactor':
                    parsed = re.compile('\d+').search(value)
                    if parsed:
                        self.vSizeFactor = int(parsed.group(0))

            line = self.stream.readline()
        
        if self.selectType == 'select/cons_res':
            if 'CR_CPU' in self.selectParams:
                self.slotType = 'CPU'
            if 'CR_Socket' in self.selectParams:
                self.slotType = 'SOCKET'
            if 'CR_Core' in self.selectParams:
                self.slotType = 'CORE'

def parseConfiguration(filename=None):
    if filename:
        cmd = shlex.split('cat ' + filename)
    else:
        cmd = shlex.split('scontrol show config')
    
    container = ConfigInfoHandler()
    CommonUtils.parseStream(cmd, container)
    return container











class PartitionInfo:

    def __init__(self):
        self.maxMem = -1
        self.defaultMem = -1

    def __str__(self):
        return "%d %d" % (self.maxMem, self.defaultMem)


class PartitionInfoHandler(Thread):

    def __init__(self):
        Thread.__init__(self)
        self.errList = list()
        self.qtable = dict()
        
        self.partRegex = re.compile('PartitionName=([^\s]+)')
        self.maxMemRegex = re.compile('MaxMemPerNode=([0-9]+)')
        self.defMemRegex = re.compile('DefMemPerNode=([0-9]+)')
        self.maxNodeRegex = re.compile('MaxNodes=([0-9]+)')
        
    def setStream(self, stream):
        self.stream = stream

    def __getitem__(self, idx):
        return self.qtable[idx]
        
    def __contains__(self, item):
        return item in self.qtable
    
    def run(self):
        line = self.stream.readline()
        
        while line:
            try:

                maxMem = -1
                defaultMem = -1
                maxNodes = -1
                
                parsed = self.partRegex.search(line)
                if not parsed:
                    continue
                queue = parsed.group(1)

                parsed = self.maxMemRegex.search(line)
                if parsed:
                    maxMem = int(parsed.group(1))                    

                parsed = self.defMemRegex.search(line)
                if parsed:
                    defaultMem = int(parsed.group(1))

                parsed = self.maxNodeRegex.search(line)
                if parsed:
                    maxNodes = int(parsed.group(1))
                
                if not queue in self.qtable:
                    self.qtable[queue] = PartitionInfo()
                
                if maxMem <> -1 and maxNodes <> -1:
                    self.qtable[queue].maxMem = maxMem * maxNodes
                if defaultMem <> -1 and maxNodes <> -1:
                    self.qtable[queue].defaultMem = defaultMem * maxNodes

            finally:
                line = self.stream.readline()

    # end of thread

def parsePartInfo(filename=None):

    if filename:
        cmd = shlex.split('cat ' + filename)
    else:
        cmd = shlex.split('scontrol -o show partitions')
    
    container = PartitionInfoHandler()
    CommonUtils.parseStream(cmd, container)
    return container

