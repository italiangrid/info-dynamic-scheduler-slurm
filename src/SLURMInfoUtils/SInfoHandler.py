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


class ErrorHandler(Thread):

    def __init__(self, err_stream):
        Thread.__init__(self)
        self.stream = err_stream
        self.message = ""
    
    def run(self):
        line = self.stream.readline()
        while line:
            self.message = self.message + line
            line = self.stream.readline()


class PartitionInfoHandler(Thread):

    def __init__(self, stream):
        Thread.__init__(self)
        self.stream = stream
        
    def run(self):
        line = self.stream.readline()
        
        while line:
            pass
    # end of thread




def parse(filename=None):

    outformat="%P "

    try:
        if filename:
            cmd = shlex.split('cat ' + filename)
        else:
            cmd = shlex.split('sinfo -o %s' % outformat)
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
        stdout_thread = PartitionInfoHandler(process.stdout)
        stderr_thread = ErrorHandler(process.stderr)
    
        stdout_thread.start()
        stderr_thread.start()
    
        ret_code = process.wait()
    
        stdout_thread.join()
        stderr_thread.join()
        
        if ret_code <> 0:
            raise Exception(stderr_thread.message)
            
        if len(stdout_thread.errList) > 0:
            raise Exception(stdout_thread.errList[0])
            
        return stdout_thread

    except:
        etype, evalue, etraceback = sys.exc_info()
        raise Exception("%s: (%s)" % (etype, evalue))


