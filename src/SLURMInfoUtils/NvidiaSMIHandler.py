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
import logging
from threading import Thread

from SLURMInfoUtils import CommonUtils

logger = logging.getLogger("NvidiaSMIHandler")


class GPUInfoHandler(Thread):

    def __init__(self):
        Thread.__init__(self)
        self.errList = list()

    def setStream(self, stream):
        self.stream = stream
        self.num_of_procs = dict()
      
    def run(self):

        try:
            line = self.stream.readline()
            while line:
                tmptuple = line.strip().split(',')
                if len(tmptuple) == 2:
                    gpu_uuid = tmptuple[0].strip()
                    if not gpu_uuid in self.num_of_procs:
                        self.num_of_procs[gpu_uuid] = 1
                    else:
                        self.num_of_procs[gpu_uuid] += 1

                line = self.stream.readline()
        except:
            logger.debug("Error parsing nvidia-smi output", exc_info=True)
            self.errList.append(CommonUtils.errorMsgFromTrace())


def parseGPUInfo(cudaHost, filename=None):

    if filename:
        cmd = shlex.split('cat ' + filename)
    else:
        smi_cmd = '"nvidia-smi --query-compute-apps=gpu_uuid,pid --format=csv,noheader"'
        ssh_opts = '-o PasswordAuthentication=no'
        cmd = shlex.split('ssh %s %s %s' % (ssh_opts, cudaHost, smi_cmd))
            
    logger.debug("Calling executable: " + repr(cmd))

    container = GPUInfoHandler()
    CommonUtils.parseStream(cmd, container)
    return container

