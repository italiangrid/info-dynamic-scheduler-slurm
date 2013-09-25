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


import time

from SLURMInfoUtils import CommonUtils

def process(config, out, infoContainer, acctContainer, slurmCfg):
    
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    glue2QueueTable, managerTable = CommonUtils.parseLdif(config["bdii-configfile"], 'GLUE2')
            
    for managerDN in managerTable:
        
        out.write(managerDN + '\n')
        out.write('GLUE2ManagerProductVersion: %s\n' % slurmCfg.version)
        out.write('GLUE2EntityCreationTime: %s\n' % now)
        out.write('\n')

    for glue2DN in glue2QueueTable:
        queue = glue2QueueTable[glue2DN]
        if not queue in infoContainer:
            continue
        qInfo = infoContainer[queue]
            
        out.write(glue2DN + '\n')
            
        if qInfo.defaultRuntime <> -1:
            out.write('GLUE2ComputingShareDefaultWallTime: %d\n' % qInfo.defaultRuntime)
                
        if qInfo.maxRuntime <> -1:
            out.write('GLUE2ComputingShareMaxWallTime: %d\n' % qInfo.maxRuntime)

        if qInfo.slotsPerJob <> -1:
            out.write('GLUE2ComputingShareMaxSlotsPerJob: %d\n' % qInfo.slotsPerJob)
                
        if CommonUtils.interfaceIsOff(config):    
            out.write('GLUE2ComputingShareServingState: draining\n')
        else:
            out.write('GLUE2ComputingShareServingState: %s\n' % qInfo.state.lower())

        out.write('GLUE2EntityCreationTime: %s\n' % now)
        out.write('\n')


