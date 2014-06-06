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
import logging

from SLURMInfoUtils import CommonUtils

MAX_INT32 = 2**31-1

def process(config, infoContainer, acctContainer, slurmCfg):

    out = sys.stdout

    logger = logging.getLogger("GLUE1Handler")
    
    glue1CETable = CommonUtils.parseLdif(config["bdii-configfile"], 'GLUE1')
    
    for glue1CEData in glue1CETable.values():
        
        glue1DN = glue1CEData['dn']
        queue = glue1CEData['queue']
            
        ceDefaultWallTime = CommonUtils.UNDEFMAXITEM
        ceMaxWallTime = CommonUtils.UNDEFMAXITEM
        ceSlotsPerJob = CommonUtils.UNDEFMAXITEM
        ceTotCPU = CommonUtils.UNDEFMAXITEM
        ceFreeCPU = CommonUtils.UNDEFMAXITEM
        ceActiveCPU = CommonUtils.UNDEFMAXITEM
        ceState = 'UNDEFINED'
        cePriority = CommonUtils.UNDEFPRIORITY
        ceMaxRunJobs = CommonUtils.UNDEFMAXITEM
        ceMaxTotJobs = CommonUtils.UNDEFMAXITEM
        ceMaxCPUTime = CommonUtils.UNDEFMAXITEM
            
        #
        # Retrieve infos from slurm core
        #
        if queue in infoContainer:
            qInfo = infoContainer[queue]
            
            ceDefaultWallTime = qInfo.defaultRuntime
            ceMaxWallTime = qInfo.maxRuntime
            ceSlotsPerJob = qInfo.slotsPerJob
            ceTotCPU = qInfo.totalCPU
            ceActiveCPU = qInfo.activeCPU
            ceFreeCPU = qInfo.freeCPU
            ceState = qInfo.state

        #
        # Retrieve infos from accounting (if available)
        #
        if acctContainer <> None:
            try:
                policyData = acctContainer.policyTable[None, queue]
                if policyData.maxWallTime <> CommonUtils.UNDEFMAXITEM:
                    ceMaxWallTime = policyData.maxWallTime
                if policyData.maxRunJobs <> CommonUtils.UNDEFMAXITEM:
                    ceMaxRunJobs = policyData.maxRunJobs
                if policyData.maxTotJobs <> CommonUtils.UNDEFMAXITEM:
                    ceMaxTotJobs = policyData.maxTotJobs
                if policyData.maxCPUTime <> CommonUtils.UNDEFMAXITEM:
                    ceMaxCPUTime = policyData.maxCPUTime
                if policyData.maxCPUPerJob <> CommonUtils.UNDEFMAXITEM:
                    ceSlotsPerJob = policyData.maxCPUPerJob
                if policyData.priority <> CommonUtils.UNDEFPRIORITY:
                    cePriority = policyData.priority
            except:
                logger.debug("No policy from accounting", exc_info=True)

            
        out.write(glue1DN + '\n')

        out.write('GlueCEInfoLRMSVersion: %s\n' % slurmCfg.version)

        if ceTotCPU <> CommonUtils.UNDEFMAXITEM:
            out.write('GlueCEInfoTotalCPUs: %d\n' % ceTotCPU)

        if ceActiveCPU <> CommonUtils.UNDEFMAXITEM:
            out.write('GlueCEPolicyAssignedJobSlots: %d\n' % ceActiveCPU)
            
        if ceFreeCPU <> CommonUtils.UNDEFMAXITEM:
            out.write('GlueCEStateFreeCPUs: %d\n' % ceFreeCPU)
            out.write('GlueCEStateFreeJobSlots: %d\n' % ceFreeCPU)
            
        if ceMaxCPUTime <> CommonUtils.UNDEFMAXITEM:
            out.write('GlueCEPolicyMaxCPUTime: %d\n' % (ceMaxCPUTime / 60))
            out.write('GlueCEPolicyMaxObtainableCPUTime: %d\n' % (ceMaxCPUTime / 60))
        else:
            out.write('GlueCEPolicyMaxCPUTime: %d\n' % MAX_INT32)
            out.write('GlueCEPolicyMaxObtainableCPUTime: %d\n' % MAX_INT32)
            
        if ceMaxTotJobs <> -1:
            out.write('GlueCEPolicyMaxTotalJobs: %d\n' % ceMaxTotJobs)
        else:
            out.write('GlueCEPolicyMaxTotalJobs: %d\n' % MAX_INT32)
            
        if ceMaxRunJobs <> CommonUtils.UNDEFMAXITEM:
            out.write('GlueCEPolicyMaxRunningJobs: %d\n' % ceMaxRunJobs)
        else:
            out.write('GlueCEPolicyMaxRunningJobs: %d\n' % MAX_INT32)
                
        if ceMaxRunJobs <> CommonUtils.UNDEFMAXITEM and ceMaxTotJobs > ceMaxRunJobs:
            out.write('GlueCEPolicyMaxWaitingJobs: %d\n' % (ceMaxTotJobs - ceMaxRunJobs))
        else:
            out.write('GlueCEPolicyMaxWaitingJobs: %d\n' % MAX_INT32)
            
        if cePriority <> CommonUtils.UNDEFPRIORITY:
            out.write('GlueCEPolicyPriority: %d\n' % cePriority)
        else:
            out.write('GlueCEPolicyPriority: %s\n' % MAX_INT32)

        #
        # For reference see https://savannah.cern.ch/bugs/?17325
        #
        if ceDefaultWallTime <> CommonUtils.UNDEFMAXITEM:
            out.write('GlueCEPolicyMaxWallClockTime: %d\n' % (ceDefaultWallTime / 60))
        else:
            out.write('GlueCEPolicyMaxWallClockTime: %d\n' % MAX_INT32)

        if ceMaxWallTime <> CommonUtils.UNDEFMAXITEM:
            out.write('GlueCEPolicyMaxObtainableWallClockTime: %d\n' % (ceMaxWallTime / 60))
        else:
            out.write('GlueCEPolicyMaxObtainableWallClockTime: %d\n' % MAX_INT32)
                
        if ceSlotsPerJob <> CommonUtils.UNDEFMAXITEM:
            out.write('GlueCEPolicyMaxSlotsPerJob: %d\n' % ceSlotsPerJob)
        else:
            out.write('GlueCEPolicyMaxSlotsPerJob: %d\n' % MAX_INT32)

        if CommonUtils.interfaceIsOff(config):
            out.write('GlueCEStateStatus: Draining\n')
        else:
            out.write('GlueCEStateStatus: %s\n' % ceState)

        out.write('\n')
            
            
            
            
        for glue1ViewData in glue1CEData['views']:
                
            viewDN = glue1ViewData[0]
            voName = glue1ViewData[1]
                
            vMaxWallTime = ceMaxWallTime
            vMaxCPUTime = ceMaxCPUTime
            vMaxRunJobs = ceMaxRunJobs
            vMaxTotJobs = ceMaxTotJobs
            vMaxCPUPerJob = ceSlotsPerJob
            vPriority = cePriority

            if acctContainer <> None:
                try:
                    tmpPol = acctContainer.policyTable[voName, queue]
                    if tmpPol.maxWallTime <> CommonUtils.UNDEFMAXITEM:
                        vMaxWallTime = tmpPol.maxWallTime
                    if tmpPol.maxCPUTime <> CommonUtils.UNDEFMAXITEM:
                        vMaxCPUTime = tmpPol.maxCPUTime
                    if tmpPol.maxRunJobs <> CommonUtils.UNDEFMAXITEM:
                        vMaxRunJobs = tmpPol.maxRunJobs
                    if tmpPol.maxTotJobs <> CommonUtils.UNDEFMAXITEM:
                        vMaxTotJobs = tmpPol.maxTotJobs
                    if tmpPol.maxCPUPerJob <> CommonUtils.UNDEFMAXITEM:
                        vMaxCPUPerJob = tmpPol.maxCPUPerJob
                    if tmpPol.priority <> CommonUtils.UNDEFPRIORITY:
                        vPriority = tmpPol.priority
                except:
                    logger.debug("No policy from accounting", exc_info=True)
                
            out.write(viewDN + '\n')
            if ceTotCPU <> CommonUtils.UNDEFMAXITEM:
                out.write('GlueCEInfoTotalCPUs: %d\n' % ceTotCPU)
                
            if ceActiveCPU <> CommonUtils.UNDEFMAXITEM:
                out.write('GlueCEPolicyAssignedJobSlots: %d\n' % ceActiveCPU)
                
            if ceFreeCPU <> CommonUtils.UNDEFMAXITEM:
                out.write('GlueCEStateFreeCPUs: %d\n' % ceFreeCPU)
                out.write('GlueCEStateFreeJobSlots: %d\n' % ceFreeCPU)

            if vPriority <> CommonUtils.UNDEFPRIORITY:
                out.write('GlueCEPolicyPriority: %d\n' % vPriority)
            else:
                out.write('GlueCEPolicyPriority: %s\n' % MAX_INT32)
                 
            if vMaxRunJobs <> CommonUtils.UNDEFMAXITEM:
                out.write('GlueCEPolicyMaxRunningJobs: %d\n' % vMaxRunJobs)
            else:
                out.write('GlueCEPolicyMaxRunningJobs: %d\n' % MAX_INT32)
                
            if vMaxTotJobs <> CommonUtils.UNDEFMAXITEM:
                out.write('GlueCEPolicyMaxTotalJobs: %d\n' % vMaxTotJobs)
            else:
                out.write('GlueCEPolicyMaxTotalJobs: %d\n' % MAX_INT32)
                
            if vMaxRunJobs <> CommonUtils.UNDEFMAXITEM and vMaxTotJobs > vMaxRunJobs:
                out.write('GlueCEPolicyMaxWaitingJobs: %d\n' % (vMaxTotJobs - vMaxRunJobs))
            else:
                out.write('GlueCEPolicyMaxWaitingJobs: %d\n' % MAX_INT32)

            if ceDefaultWallTime <> CommonUtils.UNDEFMAXITEM:
                out.write('GlueCEPolicyMaxWallClockTime: %d\n' % (ceDefaultWallTime / 60))
            else:
                out.write('GlueCEPolicyMaxWallClockTime: %d\n' % MAX_INT32)

            if vMaxWallTime <> CommonUtils.UNDEFMAXITEM:
                out.write('GlueCEPolicyMaxObtainableWallClockTime: %d\n' % (vMaxWallTime / 60))
            else:
                out.write('GlueCEPolicyMaxObtainableWallClockTime: %d\n' % MAX_INT32)
                
            if vMaxCPUTime <> CommonUtils.UNDEFMAXITEM:
                out.write('GlueCEPolicyMaxCPUTime: %d\n' % (vMaxCPUTime / 60))
                out.write('GlueCEPolicyMaxObtainableCPUTime: %d\n' % (vMaxCPUTime / 60))
            else:
                out.write('GlueCEPolicyMaxCPUTime: %d\n' % MAX_INT32)
                out.write('GlueCEPolicyMaxObtainableCPUTime: %d\n' % MAX_INT32)
                
            if vMaxCPUPerJob <> CommonUtils.UNDEFMAXITEM:
                out.write('GlueCEPolicyMaxSlotsPerJob: %d\n' % vMaxCPUPerJob)
            else:
                out.write('GlueCEPolicyMaxSlotsPerJob: %d\n' % MAX_INT32)
                
            out.write('\n')
    
    
    

