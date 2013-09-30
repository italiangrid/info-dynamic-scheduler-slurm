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
import logging

from SLURMInfoUtils import CommonUtils

def process(config, out, infoContainer, acctContainer, slurmCfg):
    
    logger = logging.getLogger("GLUE2Handler")
    
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    glue2QueueTable, managerTable = CommonUtils.parseLdif(config["bdii-configfile"], 'GLUE2')
            
    for managerDN in managerTable:
        
        out.write(managerDN + '\n')
        out.write('GLUE2ManagerProductVersion: %s\n' % slurmCfg.version)
        out.write('GLUE2EntityCreationTime: %s\n' % now)
        out.write('\n')

    for glue2ShareData in glue2QueueTable.values():
    
        glue2DN = glue2ShareData['dn']
        queue = glue2ShareData['queue']
        voname = glue2ShareData['vo']
                
        defaultWallTime = CommonUtils.UNDEFMAXITEM
        maxWallTime = CommonUtils.UNDEFMAXITEM
        slotsPerJob = CommonUtils.UNDEFMAXITEM
        queueState = 'UNDEFINED'
        maxRunJobs = CommonUtils.UNDEFMAXITEM
        maxTotJobs = CommonUtils.UNDEFMAXITEM
        maxCPUTime = CommonUtils.UNDEFMAXITEM
            
        #
        # Retrieve infos from slurm core
        #
        if queue in infoContainer:
            qInfo = infoContainer[queue]
            
            defaultWallTime = qInfo.defaultRuntime
            maxWallTime = qInfo.maxRuntime
            slotsPerJob = qInfo.slotsPerJob
            queueState = qInfo.state.lower()

        #
        # Retrieve infos from accounting (if available)
        #
        if acctContainer <> None:
            try:
                policyData = acctContainer.policyTable[voname, queue]
                defaultWallTime = policyData.maxWallTime               #???
                maxWallTime = policyData.maxWallTime
                maxRunJobs = policyData.maxRunJobs
                maxTotJobs = policyData.maxTotJobs
                maxCPUTime = policyData.maxCPUTime
            except:
                logger.debug("No policy from accounting", exc_info=True)

        out.write(glue2DN + '\n')
            
        if maxCPUTime <> CommonUtils.UNDEFMAXITEM:
            out.write('GLUE2ComputingShareDefaultCPUTime: %d\n' % maxCPUTime)
            out.write('GLUE2ComputingShareMaxCPUTime: %d\n' % maxCPUTime)
                
        if defaultWallTime <> CommonUtils.UNDEFMAXITEM:
            out.write('GLUE2ComputingShareDefaultWallTime: %d\n' % defaultWallTime)
                
        if maxWallTime <> CommonUtils.UNDEFMAXITEM:
            out.write('GLUE2ComputingShareMaxWallTime: %d\n' % maxWallTime)

        if slotsPerJob <> CommonUtils.UNDEFMAXITEM:
            out.write('GLUE2ComputingShareMaxSlotsPerJob: %d\n' % slotsPerJob)
        
        if maxTotJobs <> CommonUtils.UNDEFMAXITEM:
            out.write('GLUE2ComputingShareMaxTotalJobs: %d\n' % maxTotJobs)

        if maxRunJobs <> CommonUtils.UNDEFMAXITEM:
            out.write('GLUE2ComputingShareMaxRunningJobs: %d\n' % maxRunJobs)

        if maxRunJobs <> CommonUtils.UNDEFMAXITEM  and maxTotJobs > maxRunJobs:
            out.write('GLUE2ComputingShareMaxWaitingJobs: %d\n' % (maxTotJobs - maxRunJobs))
                        
        out.write('GLUE2ComputingShareServingState: %s\n' % queueState)

        out.write('GLUE2EntityCreationTime: %s\n' % now)
        out.write('\n')


