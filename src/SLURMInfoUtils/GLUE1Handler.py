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



from SLURMInfoUtils import CommonUtils

def process(config, out, infoContainer, acctContainer, slurmCfg):

    glue1CETable = CommonUtils.parseLdif(config["bdii-configfile"], 'GLUE1')
    
    for glue1CEData in glue1CETable.values():
        
        glue1DN = glue1CEData['dn']
        queue = glue1CEData['queue']
            
        ceDefaultWallTime = CommonUtils.UNDEFMAXITEM
        ceMaxWallTime = CommonUtils.UNDEFMAXITEM
        ceSlotsPerJob = CommonUtils.UNDEFMAXITEM
        ceTotCPU = CommonUtils.UNDEFMAXITEM
        ceFreeCPU = CommonUtils.UNDEFMAXITEM
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
            ceFreeCPU = qInfo.freeCPU
            ceState = qInfo.state

        #
        # Retrieve infos from accounting (if available)
        #
        try:
            policyData = acctContainer.policyTable[None, queue]
            if policyData.maxWallTime <> CommonUtils.UNDEFMAXITEM:
                ceDefaultWallTime = policyData.maxWallTime
                ceMaxWallTime = policyData.maxWallTime
            if policyData.maxRunJobs <> CommonUtils.UNDEFMAXITEM:
                ceMaxRunJobs = policyData.maxRunJobs
            if policyData.maxTotJobs <> CommonUtils.UNDEFMAXITEM:
                ceMaxTotJobs = policyData.maxTotJobs
            if policyData.maxCPUTime <> CommonUtils.UNDEFMAXITEM:
                ceMaxCPUTime = policyData.maxCPUTime
        except:
            #
            # TODO missing log
            #
            pass

            
        out.write(glue1DN + '\n')

        out.write('GlueCEInfoLRMSVersion: %s\n' % slurmCfg.version)

        if ceTotCPU <> CommonUtils.UNDEFMAXITEM:
            out.write('GlueCEInfoTotalCPUs: %d\n' % ceTotCPU)
            out.write('GlueCEPolicyAssignedJobSlots: %d\n' % ceTotCPU)
            
        if ceFreeCPU <> CommonUtils.UNDEFMAXITEM:
            out.write('GlueCEStateFreeCPUs: %d\n' % ceFreeCPU)
            
        if ceMaxCPUTime <> CommonUtils.UNDEFMAXITEM:
            out.write('GlueCEPolicyMaxCPUTime: %d\n' % (ceMaxCPUTime / 60))
            out.write('GlueCEPolicyMaxObtainableCPUTime: %d\n' % (ceMaxCPUTime / 60))
            
        if ceMaxTotJobs <> -1:
            out.write('GlueCEPolicyMaxTotalJobs: %d\n' % ceMaxTotJobs)
            
        if ceMaxRunJobs <> CommonUtils.UNDEFMAXITEM:
            out.write('GlueCEPolicyMaxRunningJobs: %d\n' % ceMaxRunJobs)
            
        if ceMaxRunJobs <> CommonUtils.UNDEFMAXITEM and ceMaxTotJobs > ceMaxRunJobs:
            out.write('GlueCEPolicyMaxWaitingJobs: %d\n' % (ceMaxTotJobs - ceMaxRunJobs))
            
        if cePriority <> CommonUtils.UNDEFPRIORITY:
            out.write('GlueCEPolicyPriority: %d\n' % cePriority)

        if ceDefaultWallTime <> CommonUtils.UNDEFMAXITEM:
            out.write('GlueCEPolicyMaxWallClockTime: %d\n' % (ceDefaultWallTime / 60))

        if ceMaxWallTime <> CommonUtils.UNDEFMAXITEM:
            out.write('GlueCEPolicyMaxObtainableWallClockTime: %d\n' % (ceMaxWallTime / 60))
                
        if ceSlotsPerJob <> CommonUtils.UNDEFMAXITEM:
            out.write('GlueCEPolicyMaxSlotsPerJob: %d\n' % ceSlotsPerJob)

        if CommonUtils.interfaceIsOff(config):
            out.write('GlueCEStateStatus: Draining\n')
        else:
            out.write('GlueCEStateStatus: %s\n' % ceState)

        out.write('\n')
            
            
            
            
        for glue1ViewData in glue1CEData['views']:
                
            viewDN = glue1ViewData[0]
            voName = glue1ViewData[1]
                
            try:
                tmpPol = acctContainer.policyTable[voName, queue]
                vMaxWallTime = tmpPol.maxWallTime
                vMaxCPUTime = tmpPol.maxCPUTime
                vMaxRunJobs = tmpPol.maxRunJobs
                vMaxTotJobs = tmpPol.maxTotJobs
                vPriority = tmpPol.priority
            except:
                vMaxWallTime = ceDefaultWallTime
                vMaxCPUTime = ceMaxCPUTime
                vMaxRunJobs = ceMaxRunJobs
                vMaxTotJobs = ceMaxTotJobs
                vPriority = cePriority
                
            out.write(viewDN + '\n')
            if ceTotCPU <> CommonUtils.UNDEFMAXITEM:
                out.write('GlueCEInfoTotalCPUs: %d\n' % ceTotCPU)
                out.write('GlueCEPolicyAssignedJobSlots: %d\n' % ceTotCPU)
                
            if ceFreeCPU <> CommonUtils.UNDEFMAXITEM:
                out.write('GlueCEStateFreeCPUs: %d\n' % ceFreeCPU)

            if vPriority <> CommonUtils.UNDEFPRIORITY:
                out.write('GlueCEPolicyPriority: %d\n' % vPriority)
                
            if vMaxRunJobs <> CommonUtils.UNDEFMAXITEM:
                out.write('GlueCEPolicyMaxRunningJobs: %d\n' % vMaxRunJobs)
                
            if vMaxTotJobs <> CommonUtils.UNDEFMAXITEM:
                out.write('GlueCEPolicyMaxTotalJobs: %d\n' % vMaxTotJobs)
                
            if vMaxRunJobs <> CommonUtils.UNDEFMAXITEM and vMaxTotJobs > vMaxRunJobs:
                out.write('GlueCEPolicyMaxWaitingJobs: %d\n' % (vMaxTotJobs - vMaxRunJobs))

            if vMaxWallTime <> CommonUtils.UNDEFMAXITEM:
                out.write('GlueCEPolicyMaxWallClockTime: %d\n' % (vMaxWallTime / 60))

            if ceMaxWallTime <> CommonUtils.UNDEFMAXITEM:
                out.write('GlueCEPolicyMaxObtainableWallClockTime: %d\n' % (ceMaxWallTime / 60))
                
            if vMaxCPUTime <> CommonUtils.UNDEFMAXITEM:
                out.write('GlueCEPolicyMaxCPUTime: %d\n' % (vMaxCPUTime / 60))
                out.write('GlueCEPolicyMaxObtainableCPUTime: %d\n' % (vMaxCPUTime / 60))
                
            if ceSlotsPerJob <> CommonUtils.UNDEFMAXITEM:
                out.write('GlueCEPolicyMaxSlotsPerJob: %d\n' % ceSlotsPerJob)
                
            out.write('\n')
    
    
    

