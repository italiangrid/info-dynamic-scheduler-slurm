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



if __name__ == '__main__':
    unittest.main()

