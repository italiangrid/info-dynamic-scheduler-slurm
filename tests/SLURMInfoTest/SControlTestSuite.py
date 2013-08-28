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
import unittest

from SLURMInfoUtils import SControlInfoHandler
from TestUtils import Workspace

class SControlTestCase(unittest.TestCase):

    def setUp(self):
        self.workspace = Workspace()

        self.nodePattern = '''NodeName=%(nname)s Arch=x86_64 CoresPerSocket=1 CPUAlloc=%(calloc)d CPUErr=0 CPUTot=%(ctot)d CPULoad=0.00 Features=(null) Gres=(null) NodeAddr=%(nname)s NodeHostName=%(nname)s OS=Linux RealMemory=1 AllocMem=0 Sockets=2 Boards=1 State=%(nstate)s ThreadsPerCore=1 TmpDisk=0 Weight=1 BootTime=2013-08-23T09:49:03 SlurmdStartTime=2013-08-23T10:04:46 CurrentWatts=0 LowestJoules=0 ConsumedJoules=0 ExtSensorsJoules=n/s ExtSensorsWatts=0 ExtSensorsTemp=n/s 
'''

        self.jobPattern = '''JobId=%(jid)s Name=%(jname)s UserId=%(uid)s(0) GroupId=%(gid)s(0) Priority=4294901756 Account=(null) QOS=(null) JobState=%(jstate)s Reason=None Dependency=(null) Requeue=1 Restarts=0 BatchFlag=1 ExitCode=0:0 RunTime=00:01:00 TimeLimit=%(tlimit)s TimeMin=N/A SubmitTime=%(subtime)s EligibleTime=2013-08-26T11:54:52 StartTime=%(sttime)s EndTime=2013-08-26T11:55:52 PreemptTime=None SuspendTime=None SecsPreSuspend=0 Partition=%(pname)s AllocNode:Sid=cream-04:2682 ReqNodeList=(null) ExcNodeList=(null) NodeList=cream-42 BatchHost=cream-42 NumNodes=1 NumCPUs=%(ncpu)d CPUs/Task=1 ReqS:C:T=*:*:* MinCPUsNode=1 MinMemoryNode=0 MinTmpDiskNode=0 Features=(null) Gres=(null) Reservation=(null) Shared=0 Contiguous=0 Licenses=(null) Network=(null) Command=/root/test.sh WorkDir=/root
'''

        self.configPattern = '''Configuration data as of 2013-08-28T10:34:42
SelectType              = %(seltype)s
SelectTypeParameters    = %(selpar)s
SLURM_VERSION           = %(version)s
'''

    def test_scontrol_all_free(self):
    
        pattern_args = {'nname' : 'cream-34',
                        'nstate' : 'IDLE',
                        'ctot' : 2,
                        'calloc' : 0}
        
        tmpfile = self.workspace.createFile(self.nodePattern % pattern_args)
        
        pattern_args['nname'] = 'cream-42'
        
        self.workspace.appendToFile(self.nodePattern % pattern_args, tmpfile)
        
        pattern_args['nname'] = 'cream-46'
        
        self.workspace.appendToFile(self.nodePattern % pattern_args, tmpfile)
        
        ncpu, freecpu = SControlInfoHandler.parseCPUInfo(tmpfile)

        self.assertTrue(ncpu == 6 and freecpu == 6)


    def test_scontrol_part_alloc(self):

        pattern_args = {'nname' : 'cream-34',
                        'nstate' : 'ALLOCATED+',
                        'ctot' : 4,
                        'calloc' : 4}

        tmpfile = self.workspace.createFile(self.nodePattern % pattern_args)
        
        pattern_args = {'nname' : 'cream-42',
                        'nstate' : 'IDLE',
                        'ctot' : 4,
                        'calloc' : 0}
        
        self.workspace.appendToFile(self.nodePattern % pattern_args, tmpfile)


        pattern_args = {'nname' : 'cream-46',
                        'nstate' : 'DOWN*',
                        'ctot' : 4,
                        'calloc' : 0}
        
        self.workspace.appendToFile(self.nodePattern % pattern_args, tmpfile)

        ncpu, freecpu = SControlInfoHandler.parseCPUInfo(tmpfile)

        self.assertTrue(ncpu == 8 and freecpu == 4)


    def test_scontrol_config_ok(self):
    
        pattern_args = {'version' : '2.6.0',
                        'seltype' : 'select/cons_res',
                        'selpar' : 'CR_CPU'}

        tmpfile = self.workspace.createFile(self.configPattern % pattern_args)
        
        container = SControlInfoHandler.parseConfiguration(tmpfile)
        
        result = container.selectType == 'select/cons_res'
        result = result and container.selectParams == 'CR_CPU'
        result = result and container.version == '2.6.0'
        
        self.assertTrue(result)




if __name__ == '__main__':
    unittest.main()

