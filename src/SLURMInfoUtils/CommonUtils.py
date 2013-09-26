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
import shlex
import subprocess
import traceback
import glob
import ConfigParser
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


bdiiCfgRegex = re.compile('^\s*BDII_([^=\s]+)\s*=(.+)$')

def getBDIIConfig(bdiiConffile):

    result = dict()
    
    cFile = None
    try:
        cFile = open(bdiiConffile)
        
        for line in cFile:
            parsed = bdiiCfgRegex.match(line)
            if parsed:
                result[parsed.group(1).lower()] = parsed.group(2).strip()

    finally:
        if cFile:
            cFile.close()
    
    return result


glue1DNRegex = re.compile("dn:\s*GlueCEUniqueID\s*=\s*.+")
glue1VODNRegex = re.compile("dn:\s*GlueVOViewLocalID\s*=\s*.+")
glue1AttrRegex = re.compile("Glue([^:\s]+)\s*:\s*(.+)")

glue2DNRegex = re.compile("dn:\s*GLUE2ShareID\s*=\s*.+")
glue2ShareRegex = re.compile("GLUE2ComputingShareMappingQueue\s*:\s*(.+)")

managerRegex = re.compile("dn:\s*GLUE2ManagerId\s*=\s*.+")
manAttrRegex = re.compile("GLUE2ManagerID\s*:\s*(.+)")

def parseLdif(bdiiConffile, glueType):

    bdiiConfig = getBDIIConfig(bdiiConffile)

    if 'ldif_dir' in bdiiConfig:
        ldifDir = bdiiConfig['ldif_dir']
    else:
        ldifDir = '/var/lib/bdii/gip/ldif'
    
    ldifList = glob.glob(ldifDir + '/*.ldif')
    
    if glueType =='GLUE1':
    
        result = dict()
        
        #
        # Shortcut for old installations
        #
        scFilename = ldifDir + '/static-file-CE.ldif'
        if scFilename in ldifList:
            ldifList = [scFilename]
        
        currCEID = None
        currCEDN = None
        currQueue = None
        currVODN = None
        currVOName = None
        currVORef = None

        for ldifFilename in ldifList:
        
            ldifFile = None
            
            try:
            
                ldifFile = open(ldifFilename)
                for line in ldifFile:
                    parsed = glue1DNRegex.match(line)
                    if parsed:
                        currCEDN = line.strip()
                        continue
                    
                    parsed = glue1VODNRegex.match(line)
                    if parsed:
                        currVODN = line.strip()
                        continue

                    parsed = glue1AttrRegex.match(line)
                    if parsed:
                        
                        if parsed.group(1) == 'CEUniqueID':
                            currCEID = parsed.group(2).strip()
                            continue
                    
                        if parsed.group(1) == 'CEName':
                            currQueue = parsed.group(2).strip()
                            continue

                        if parsed.group(1) == 'VOViewLocalID':
                            currVOName = parsed.group(2).strip()
                            continue
                    
                        if parsed.group(1) == 'ChunkKey':
                            chunkKey = parsed.group(2).strip()
                            if chunkKey.startswith('GlueCEUniqueID='):
                                currVORef = chunkKey[15:]
                            continue

                    if len(line.strip()) == 0:
                        if currCEID:
                            if not currCEID in result:
                                result[currCEID] = { 'views' : list() }
                            result[currCEID]['dn'] = currCEDN
                            result[currCEID]['queue'] = currQueue
                        
                        if currVORef:
                            if not currVORef in result:
                                result[currVORef] = { 'views' : list() }
                            result[currVORef]['views'].append((currVODN, currVOName))
                            
                        currCEID = None
                        currCEDN = None
                        currQueue = None
                        currVODN = None
                        currVOName = None
                        currVORef = None

            finally:
                if ldifFile:
                    ldifFile.close()
        
        if currCEID:
            if not currCEID in result:
                result[currCEID] = { 'views' : list() }
            result[currCEID]['dn'] = currCEDN
            result[currCEID]['queue'] = currQueue
                        
        if currVORef:
            if not currVORef in result:
                result[currVORef] = { 'views' : list() }
            result[currVORef]['views'].append((currVODN, currVOName))
        
    else:
    
        result = (dict(), dict())

        #
        # Shortcut for old installations
        #
        scFilename1 = ldifDir + '/ComputingManager.ldif'
        scFilename2 = ldifDir + '/ComputingShare.ldif'
        if scFilename1 in ldifList and scFilename2 in ldifList:
            ldifList = [scFilename1, scFilename2]

        for ldifFilename in ldifList:
        
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
                        result[0][currDN1] = parsed.group(1).strip()
                        continue
                    
                    parsed = managerRegex.match(line)
                    if parsed:
                        currDN2 = line.strip()
                        continue
                    
                    parsed = manAttrRegex.match(line)
                    if parsed and currDN2:
                        result[1][currDN2] = parsed.group(1).strip()
                        continue
                    
                    if len(line.strip()) == 0:
                        currDN1 = None
                        currDN2 = None

            finally:
                if ldifFile:
                    ldifFile.close()

    return result

def readConfigFile(configFile):

    pRegex = re.compile('^\s*([^=\s]+)\s*=(.+)$')
    conffile = None
    config = dict()
    
    newFormat = False
    
    try:
    
        conffile = open(configFile)
        for line in conffile:
            parsed = pRegex.match(line)
            if parsed:
                config[parsed.group(1)] = parsed.group(2).strip(' \n\t"')
            else:
                tmps = line.strip()
                if tmps.startswith('['):
                    newFormat = True
                    break  
                elif len(tmps) > 0 and not tmps.startswith('#'):
                    raise Exception("Error parsing configuration file " + configFile)

        if newFormat:
            conffile.seek(0)
            tmpConf = ConfigParser.ConfigParser()
            tmpConf.readfp(conffile)
            
            if tmpConf.has_option('Main','outputformat'):
                config['outputformat'] = tmpConf.get('Main', 'outputformat')
                
            if tmpConf.has_option('Main','bdii-configfile'):
                config['bdii-configfile'] = tmpConf.get('Main', 'bdii-configfile')
                
            if tmpConf.has_option('WSInterface','status-probe'):
                config['status-probe'] = tmpConf.get('WSInterface', 'status-probe')

    finally:
        if conffile:
            conffile.close()

    if not "outputformat" in config:
        if "GlueFormat" in config:
            config["outputformat"] = config["GlueFormat"]
        else:
            config["outputformat"] = "both"
    
    if config["outputformat"] not in ["glue1", "glue2", "both"]:
        raise Exception("FATAL: Unknown output format specified in config file:%s" % config["outputformat"])

    if not "bdii-configfile" in config:
        config["bdii-configfile"] = '/etc/bdii/bdii.conf'
            

    return config


def convertTimeLimit(tstr):

    tmpl = tstr.split('-')
    if len(tmpl) > 1:
        tmpl2 = tmpl[1].split(':')
        
        if len(tmpl2) > 2:
            return int(tmpl[0]) * 86400 + int(tmpl2[0]) * 3600 + int(tmpl2[1]) * 60 + int(tmpl2[2])
        
        if len(tmpl2) > 1:
            return int(tmpl[0]) * 86400 + int(tmpl2[0]) * 3600 + int(tmpl2[1]) * 60
        
        return int(tmpl[0]) * 86400 + int(tmpl2[0]) * 3600
        
    else:
        tmpl2 = tmpl[0].split(':')
        
        if len(tmpl2) > 2:
            return int(tmpl2[0]) * 3600 + int(tmpl2[1]) *60 + int(tmpl2[2])
        
        if len(tmpl2) > 1:
            return int(tmpl2[0]) * 60 + int(tmpl2[1])
        
        return int(tmpl2[0]) * 60


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

def interfaceIsOff(config):
    try:
    
        if 'status-probe' in config:
            retcode = subprocess.call(shlex.split(config['status-probe']))
            return retcode == 1 or retcode == 2
        
    except:
        pass
    
    return False


