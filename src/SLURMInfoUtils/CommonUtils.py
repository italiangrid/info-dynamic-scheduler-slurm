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
import subprocess
import traceback
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


def parseStream(cmd, container):

    processErr = None
    
    try:
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
        container.setStream(process.stdout)
        stderr_thread = ErrorHandler(process.stderr)
    
        container.start()
        stderr_thread.start()
    
        ret_code = process.wait()
    
        container.join()
        stderr_thread.join()
        
        if ret_code <> 0:
            processErr = stderr_thread.message
            
        if len(container.errList) > 0:
            processErr = container.errList[0]

    except:
        raise Exception(errorMsgFromTrace())

    if processErr:
        raise Exception(processErr)


bdiiCfgRegex = re.compile('^\s*BDII_([^=\s]+)\s*=([^$]+)$')

def getBDIIConfig(bdiiConffile):

    result = dict()
    
    cFile = None
    try:
        cFile = open(bdiiConffile)
        
        for line in cFile:
            parsed = bdiiCfgRegex.match(line)
            if parsed:
                result[parsed.group(1).lower] = parsed.group(2).strip()

    finally:
        if cFile:
            cFile.close()


glue1DNRegex = re.compile("dn:\s*GlueCEUniqueID\s*=\s*[^$]+")
glue1QueueRegex = re.compile("GlueCEName\s*:\s*([^$]+)")

glue2DNRegex = re.compile("dn:\s*GLUE2ShareID\s*=\s*[^$]+")
glue2ShareRegex = re.compile("GLUE2ComputingShareMappingQueue\s*:\s*([^$]+)")

managerRegex = re.compile("dn:\s*GLUE2ManagerId\s*=\s*[^$]+")
manAttrRegex = re.compile("GLUE2ManagerID\s*:\s*([^$]+)")

def parseLdif(bdiiConffile, glueType):

    bdiiConfig = getBDIIConfig(bdiiConffile)

    if 'ldif_dir' in bdiiConfig:
        ldifDir = bdiiConfig['ldifDir']
    else:
        ldifDir = '/var/lib/bdii/gip/ldif'
    
    ldifList = glob.glob(ldifDir + '*.ldif')
    
    if glueType =='GLUE1':
    
        result = dict()
        
        for ldiFilename in ldifList:
        
            ldifFile = None
            currDN = None
            try:
            
                ldifFile = open(ldifFilename)
                for line in ldifFile:
                    parsed = glue1DNRegex.match(line)
                    if parsed:
                        currDN = line.strip()
                        continue
                    
                    parsed = glue1QueueRegex.match(line)
                    if parsed and currDN:
                        result[currDN] = tmpm.group(1).strip()
                        continue
                    
                    if len(line.strip()) == 0:
                        currDN = None

            finally:
                if ldifFile:
                    ldifFile.close()

    else:
    
        result = (dict(), dict())

        for ldiFilename in ldifList:
        
            ldifFile = None
            currDN1 = None
            currDN2 = None
            try:
            
                ldifFile = open(ldifFilename)
                for line in ldifFile:
                    parsed = glue2DNRegex.match(line)
                    if parsed:
                        currDN1 = line.strip()
                        continue
                    
                    parsed = glue2ShareRegex.match(line)
                    if parsed and currDN1:
                        result[0][currDN1] = tmpm.group(1).strip()
                        continue
                    
                    parsed = managerRegex.match(line)
                    if parsed:
                        currDN2 = line.strip()
                        continue
                    
                    parsed = manAttrRegex.match(line)
                    if parsed and currDN2:
                        result[1][currDN2] = tmpm.group(1).strip()
                        continue
                    
                    if len(line.strip()) == 0:
                        currDN1 = None
                        currDN2 = None

            finally:
                if ldifFile:
                    ldifFile.close()

    return result

def readConfigFile(configFile):

    pRegex = re.compile('^\s*([^=\s]+)\s*=([^$]+)$')
    conffile = None
    config = dict()
    
    try:
    
        conffile = open(configFile)
        for line in conffile:
            parsed = pRegex.match(line)
            if parsed:
                config[parsed.group(1)] = parsed.group(2).strip(' \n\t"')
            else:
                tmps = line.strip()
                if len(tmps) > 0 and not tmps.startswith('#'):
                    raise Exception("Error parsing configuration file " + configFile)

    finally:
        if conffile:
            conffile.close()

    return config


def convertTimeLimit(tstr):

    # format [[[dd-]hh:]mm:]ss
    tmpl = tstr.split('-')
    if len(tmpl) > 1:
        result = int(tmpl[0]) * 86400
        hStr = tmpl[1]
    else:
        result = 0
        hStr = tmpl[0]
    
    tmpl = hStr.split(':')
    if len(tmpl) > 0:
        result += int(tmpl[-1])
    if len(tmpl) > 1:
        result += int(tmpl[-2]) * 60
    if len(tmpl) > 2:
        result += int(tmpl[-3]) * 3600
            
    return result

def convertJobSize(sstr):

    minsize = -1
    maxsize = -1
    
    # format nn[-nn|INFINITE|UNLIMITED]
    tmpl = sstr.split('-')
    if len(tmpl) > 1:
        minsize = int(tmpl[0])
        if not tmpl[1].lower() in ['infinite', 'unlimited']:
            maxsize = int(tmpl[1])
    elif len(tmpl) > 0:
        minsize = int(tmpl[0])
        maxsize = minsize
    
    return minsize, maxsize

def errorMsgFromTrace():

    etype, evalue, etraceback = sys.exc_info()
    trMessage = ''
    
    trList = traceback.extract_tb(etraceback)
    for trArgs in trList:
        if 'SLURMInfoUtils' in trArgs[0]:
            trMessage = '%s: %d' % (trArgs[0], trArgs[1])
    
    return '%s (%s)' % (evalue, trMessage)



