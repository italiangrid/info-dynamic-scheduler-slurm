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
import shlex

from SLURMInfoUtils import CommonUtils
from SLURMInfoUtils import SAcctMgrHandler
from TestUtils import Workspace

def parsePolicies(filename):
    cmd = shlex.split('cat ' + filename)
    container = SAcctMgrHandler.PolicyInfoHandler()
    CommonUtils.parseStream(cmd, container)
    return container


class SAcctMgrTestCase(unittest.TestCase):

    def setUp(self):
        self.workspace = Workspace()
        
        self.headerPattern = 'Cluster|Account|User|Partition|'
        self.headerPattern += 'Share|GrpJobs|GrpNodes|GrpCPUs|GrpMem|GrpSubmit|GrpWall|GrpCPUMins|'
        self.headerPattern += 'MaxJobs|MaxNodes|MaxCPUs|MaxSubmit|MaxWall|MaxCPUMins|QOS|Def QOS\n'


    def test_policies_parsing_ok(self):
        
        tmpbuff = self.headerPattern
        tmpbuff += 'clusteroncream04|padova|slurm|creamtest2|1||||||||20||||1-12|1440|normal|\n'
        tmpbuff += 'clusteroncream04|padova|slurm|creamtest1|1||||||||20||||1-12|1440|normal|\n'
        tmpbuff += 'clusteroncream04|padova|dteam|creamtest1|1||||||||20||||12:00:00|2880|normal|\n'
        tmpbuff += 'clusteroncream04|padova|atlas||1||||||||20||||12:00:00|2880|normal|\n'
        
        tmpfile = self.workspace.createFile(tmpbuff)
        
        container = parsePolicies(tmpfile)
        
        
        try:
            tmpPol = container.policyTable['slurm', 'creamtest2']
            
            result = tmpPol.maxWallTime == 129600 and tmpPol.maxCPUTime == 86400
        
            tmpPol = container.policyTable[None, 'creamtest1']
            
            result = result and tmpPol.maxWallTime == 129600 and tmpPol.maxCPUTime == 172800
            
            tmpPol = container.policyTable['atlas', None]
            
            result = result and tmpPol.maxWallTime == 43200 and tmpPol.maxCPUTime == 172800

        except:
            result = False
        
        self.assertTrue(result)






if __name__ == '__main__':
    unittest.main()

