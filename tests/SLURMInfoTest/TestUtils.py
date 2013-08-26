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
import os, os.path
import shutil
import tempfile


class Workspace:

    def __init__(self, **args):

        if "workspacedir" in args:
            self.workspace = args["workspacedir"]
        else:
            self.workspace = "/tmp/infoslurmtest"
        if os.path.exists(self.workspace):
            shutil.rmtree(self.workspace)
        os.mkdir(self.workspace)


    def createFile(self, data):
        tmpfd, tmpfilename = tempfile.mkstemp(".txt", "data", self.workspace)
        dataFile = os.fdopen(tmpfd, 'w')
        dataFile.write(data)
        dataFile.close()
        return tmpfilename
        
    def appendToFile(self, data, filename):
        dataFile = open(filename, 'a')
        dataFile.write(data)
        dataFile.close()



