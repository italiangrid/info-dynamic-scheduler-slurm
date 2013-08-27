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
        self.stateRegex = re.compile('State=(IDLE|COMPLETING|ALLOCATED[+]?|DRAINING|MIXED)')
        self.tcpuRegex = re.compile('CPUTot=([0-9]+)')
        self.acpuRegex = re.compile('CPUAlloc=([0-9]+)')
    
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

            finally:
                line = self.stream.readline()


def parseCPUInfo(filename=None):
    if filename:
        cmd = shlex.split('cat ' + filename)
    else:
        cmd = shlex.split('scontrol -o show nodes')
    
    container = NodesInfoHandler()
    CommonUtils.parseStream(cmd, container)
    return container.ncpu, container.nfree


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
        return int(time.mktime(time.strptime(tstr, '%Y-%m-%dT%H:%M:%S')))
    
    
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


def parseJobInfo(container, filename=None):
    if filename:
        cmd = shlex.split('cat ' + filename)
    else:
        cmd = shlex.split('scontrol -o show jobs')
    
    container = JobInfoHandler(container)
    CommonUtils.parseStream(cmd, container)


class JobInfoHandler(Thread):

    def __init__(self, container):
        Thread.__init__(self)
        self.errList = list()
        self.version = ''
        self.vRegex = re.compile('[a-zA-Z]+\s*([0-9]+\.[0-9]+\.[0-9]+)')
        
    def setStream(self, stream):
        self.stream = stream

    def run(self):
        line = self.stream.readline()
        
        while line:
        
            parsed = self.vRegex.search(line)
            if parsed:
                self.version = parsed.group(1)
            
            line = self.stream.readline()


def parseVersion():
    if filename:
        cmd = shlex.split('cat ' + filename)
    else:
        cmd = shlex.split('scontrol -V')
    
    container = VerInfoHandler(container)
    CommonUtils.parseStream(cmd, container)
    return container.version

