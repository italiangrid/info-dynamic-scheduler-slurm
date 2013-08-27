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


class PartitionInfo:

    def __init__(self):
        self.state = "unknown"
        self.maxCPUTime = -1
        self.defaultCPUTime = -1

    def __str__(self):
        return "%s %d %d" % (self.state, self.maxCPUTime, self.defaultCPUTime)

class PartitionInfoHandler(Thread):

    def __init__(self):
        Thread.__init__(self)
        self.errList = list()
        self.qtable = dict()
        
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
                           
                line = line.strip()
                
                if len(line) == 0:
                    continue
                    
                qTuple = line.split()
                
                if len(qTuple) <> 6:
                    self.errList.append("Wrong format for partition info")
                    continue
                
                queue = qTuple[0]
                if queue.endswith('*'):
                    queue = queue[:-1]
                if not queue in self.qtable:
                    self.qtable[queue] = PartitionInfo()
                    
                if qTuple[1] == 'down':
                    self.qtable[queue].state = 'Closed'
                elif qTuple[1] == 'drain':
                    self.qtable[queue].state = 'Draining'
                else:
                    self.qtable[queue].state = 'Production'
                
                if qTuple[4] <> 'n/a':
                    self.qtable[queue].maxCPUTime = CommonUtils.convertTimeLimit(qTuple[4])
                
                if qTuple[5] <> 'n/a':
                    self.qtable[queue].defaultCPUTime = CommonUtils.convertTimeLimit(qTuple[5])
                                
            finally:
                line = self.stream.readline()

    # end of thread




def parse(filename=None):

    if filename:
        cmd = shlex.split('cat ' + filename)
    else:
        cmd = shlex.split('sinfo -h -o "%P %a %C %T %l %L"')
    
    container = PartitionInfoHandler()
    CommonUtils.parseStream(cmd, container)
    return container





