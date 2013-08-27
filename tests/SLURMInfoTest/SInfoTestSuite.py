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

from SLURMInfoUtils import SInfoHandler
from TestUtils import Workspace

class SInfoTestCase(unittest.TestCase):

    def setUp(self):
        self.workspace = Workspace()
        
        self.partPattern = "%(partid)s %(state)s %(cpuinfo)s %(maxcput)s %(defcput)s\n"


    def test_partition_ok(self):
        
        pattern_args = {'partid' : 'creamtest1',
                        'state' : 'up',
                        'cpuinfo' : '0/2/0/2',
                        'maxcput' : '30:00',
                        'defcput' : 'n/a'}

        tmpfile = self.workspace.createFile(self.partPattern % pattern_args)

        pattern_args['partid'] = 'creamtest2*'
        
        self.workspace.appendToFile(self.partPattern % pattern_args, tmpfile)
        
        container = SInfoHandler.parse(tmpfile)
        
        result = container['creamtest1'].maxRuntime == 1800
        result = result and container['creamtest1'].defaultRuntime == -1
        result = result and container['creamtest1'].state == 'Production'
        
        result = result and container['creamtest2'].maxRuntime == 1800
        result = result and container['creamtest2'].defaultRuntime == -1
        result = result and container['creamtest2'].state == 'Production'

        self.assertTrue(result)


    def test_partition_one_closed(self):
        
        pattern_args = {'partid' : 'creamtest1',
                        'state' : 'down',
                        'cpuinfo' : '0/2/0/2',
                        'maxcput' : '30:00',
                        'defcput' : 'n/a'}

        tmpfile = self.workspace.createFile(self.partPattern % pattern_args)

        pattern_args['partid'] = 'creamtest2*'
        pattern_args['state'] = 'up'
        
        self.workspace.appendToFile(self.partPattern % pattern_args, tmpfile)
        self.workspace.appendToFile(self.partPattern % pattern_args, tmpfile)
        
        container = SInfoHandler.parse(tmpfile)
        
        result = container['creamtest1'].state == 'Closed'
        result = result and container['creamtest2'].state == 'Production'

        self.assertTrue(result)
    
    def test_partition_cpucount_ok(self):
    
        pattern_args = {'partid' : 'creamtest1',
                        'state' : 'up',
                        'cpuinfo' : '2/2/0/4',
                        'maxcput' : '30:00',
                        'defcput' : 'n/a'}

        tmpfile = self.workspace.createFile(self.partPattern % pattern_args)

        container = SInfoHandler.parse(tmpfile)
        
        result = container['creamtest1'].freeCPU == 2
        result = result and container['creamtest1'].totalCPU == 4
        
        self.assertTrue(result)



if __name__ == '__main__':
    unittest.main()

