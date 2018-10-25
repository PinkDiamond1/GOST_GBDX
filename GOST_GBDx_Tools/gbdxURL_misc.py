import re, requests, json, sys, os, io, configparser, logging, time
import subprocess

##STEPS to get AWS to work in WBG
# 1. Open Anaconda command prompt with awscli and osgeo installed
# 2. >> where aws ; >> where gdalbuildvrt
# 3. Copy the two folders indicated above to C:\WBG
# 4. Profit 


def tPrint(s):
    print("%s\t%s" % (time.strftime("%H:%M:%S"), s) )

class gbdxURL(object):
    def __init__(self, gbdx, wbgComp=False):
        #Get reference to gbdx username, password, in the gbdx config file
        gbdxConfigFile = os.path.join(os.path.expanduser('~'), ".gbdx-config")
        config = configparser.RawConfigParser()  
        ''' This part doesn't work in 3.6
        with open(gbdxConfigFile) as f:
            sample_config = f.read()
        config.readfp(io.BytesIO(sample_config))
        '''
        config.read(gbdxConfigFile)
        self.username = config.get('gbdx','user_name')
        self.password = config.get('gbdx','user_password')
        self.client_id = gbdx.s3.info['S3_access_key']
        self.secret_key = gbdx.s3.info['S3_secret_key']
    
        url = 'https://geobigdata.io/auth/v1/oauth/token/'
        headers = {"Authorization": "Basic %s" % self.client_id, "Content-Type": "application/x-www-form-urlencoded"}
        params = {"grant_type": "password", "username": self.username, "password": self.password }
        resultsBearer = requests.post(url, headers=headers, data=params)

        self.access_token = resultsBearer.json()['access_token']
        self.awsCommand = 'aws'
        self.generateAWSkeys()
        if wbgComp:
            self.awsCommand = 'python aws-script.py'
        
    def listAllTasks(self):
        ''' list all the tasks currently registered in GBDx
        '''
        url     = 'https://geobigdata.io/workflows/v1/tasks'
        headers = {"Authorization": "Bearer " + self.access_token}
        resultsTasks = requests.get(url,headers=headers)
        return resultsTasks.json()['tasks']
        
    def listAllTasks_Advanced(self, outFile):
        allFunctions = {}
        for x in self.listAllTasks():
            name = x.split(":")[0]
            val = int(x.split(":")[1].replace(".", ""))
            try:
                if allFunctions[name] > val:
                    allFunctions[name] = val
            except:
                allFunctions[name] = val
        with open(outFile, 'w') as outFile:
            for key, value in allFunctions.iteritems():
                curDesc = self.descTask(key)
                try:
                    outFile.write("%s,%s,%s\n" % (curDesc['name'], curDesc['version'], curDesc['description']))
                except:
                    print("%s did not search" % key)
                    print(curDesc)
    def descTask(self, task):
        ''' describe the defined task
        '''
        url     = 'https://geobigdata.io/workflows/v1/tasks/%s' % task
        headers = {"Authorization": "Bearer " + self.access_token}
        resultsTasks = requests.get(url,headers=headers)
        return resultsTasks.json()
        
    def cancelWorkflow(self, task):        
        url     = 'https://geobigdata.io/workflows/v1/workflows/:%s/cancel' % task
        headers = {"Authorization": "Bearer " + self.access_token}
        resultsTasks = requests.get(url,headers=headers)
        return resultsTasks.json()
    def getS3Creds(self):
        url = "https://geobigdata.io/s3creds/v1/prefix"
        headers = {"Authorization": "Bearer " + self.access_token}
        resultsCreds = requests.get(url, headers=headers)
        s3Creds = resultsCreds.json()
        return(s3Creds)
        
    def listWorkflows(self):
        url     = 'https://geobigdata.io/workflows/v1/workflows'
        headers = {"Authorization": "Bearer " + self.access_token}
        resultsTasks = requests.get(url,headers=headers)
        return(resultsTasks.json()['Workflows'])
    
    def descWorkflow(self, wID):        
        url     = 'https://geobigdata.io/workflows/v1/workflows/%s' % wID
        headers = {"Authorization": "Bearer " + self.access_token}
        resultsTasks = requests.get(url,headers=headers)
        return(resultsTasks.json())
        
    def generateAWSkeys(self):
        url          = 'https://geobigdata.io/s3creds/v1/prefix?duration=129600'
        headers      = {'Content-Type': 'application/json',"Authorization": "Bearer " + self.access_token}
        results      = requests.get(url,headers=headers)
        
        self.s3Creds = results.json()
        self.prefix = self.s3Creds['prefix']

        commands = []
        commands.append("set aws_secret_access_key=" + results.json()['S3_secret_key'])
        commands.append("set aws_access_key_id=" + results.json()['S3_access_key'])
        commands.append("set aws_session_token=" + results.json()['S3_session_token'])
        return (commands)
                
    def downloadS3Contents(self, s3Folder, localFolder, recursive=False):
        allCommands = self.generateAWSkeys()
        if recursive:
            allCommands.append("aws s3 cp --recursive %s %s" % (s3Folder, localFolder))
        else:
            allCommands.append("aws s3 cp %s %s" % (s3Folder, localFolder))
        return (allCommands)
        
    def listS3Contents(self, s3Folder, outFile = '', recursive=False):
        ''' List the contents of the provided s3Folder
        s3Folder [string] - path to s3 folder WITHOUT the prefix. 
                            The file path must be passed with a %s flag for the prefix
        [optional] outFile [string] - path to create an output file foir the returned commands
                                        to spit out the saved results
        [optional] recursive [boolean] - whether to do search recusively or not
        '''
        allCommands = self.generateAWSkeys()
        if recursive:
            recString = "--recursive"
        else:
            recString = ""
        if outFile == '':
            allCommands.append('%s s3 ls %s %s' % (self.awsCommand, recString, s3Folder))
        else:
            allCommands.append('%s s3 ls %s %s > %s' % (self.awsCommand, recString, s3Folder, outFile))
            
        return (allCommands)
    
    def processAwsList(self, inFile, searchVal):
        ''' process the results of the above listS3Contents
        inFile [string] - file to process
        searchVal [string] - regular expression to search for in each line of the document
        '''
        allVals = []
        with open(inFile, 'r') as inFile:
            for line in inFile:
                if re.search(searchVal, line):
                    allVals.append(line)
        return allVals
    
    def processAWS_Contents(self, inputFile, baseS3folder, outFolder, command='ls', recursive=False):
        ''' Generate AWS commands for downloading AWS contents from aws inputFile generated
            from listS3contents
        [inputFile] - s3 contents file
        [baseS3folder] - folder from which inputFile was determined
        [outFolder] - location to download to
        [RETURNS] - list of download commands
        '''
        commands = self.generateAWSkeys()
        with open(inputFile, 'r') as awsImages:
            for line in awsImages:
                curFolder = line.strip().replace("PRE ", "")    
                logging.info("Processing %s" % curFolder)
                curOutFolder = os.path.join(outFolder, curFolder)
                if not os.path.exists(outFolder):
                    os.mkdir(outFolder)
                #Download and prepare the results
                curCommand = "%s s3 %s --recursive %s %s" % (self.awsCommand, command, baseS3folder % (self.prefix, curFolder), curOutFolder)                          
                commands.append(curCommand)
        return commands
    
    def downloadAWS_file(self, file, outputFile):
        commands = self.generateAWSkeys()
        commands.append("%s s3 cp %s %s" % (self.awsCommand, file, outputFile))
        return commands
    
    def executeAWS_file(self, awsCommands, commandFile):
        with open(commandFile, 'w') as outFile:
            for l in awsCommands:
                outFile.write(l)
                outFile.write("\r\n")
                
        subprocess.call([commandFile], shell=True) 

    def monitorWorkflows(self, sleepTime=60, focalWorkflows=[]):
        ''' continuously monitor currently running workflows on GBDx
        sleepTime [integer] - abount of time to sleep between checks
        focalWorkflows [list of numbers] - specific workflows to focus on
        '''
        curWorkflows = self.listWorkflows()
        #if there are focal workflows, focus only on that
        if len(focalWorkflows) > 0:        
            mainWorkflows = []
            for rID in curWorkflows:
                if rID in focalWorkflows:
                    mainWorkflows.append(rID)
            curWorkflows = mainWorkflows
                               
        failedTasks = {}
        succeededTasks = {}
        stillProcessing = True
        startTime = time.strftime("%H:%M:%S")
        while stillProcessing:
            stillProcessing = False
            count = 0
            for rID in curWorkflows:
                r = self.descWorkflow(rID)
                try:
                    tPrint("%s - %s - %s" % (startTime, rID, r['state']['event']))
                    if r['state']['event'] in ['started','scheduled', 'running', 'submitted','rescheduling']:
                        stillProcessing = True            
                    elif r['state']['event'] == 'failed':
                        failedTasks[r['id']] = r
                    elif r['state']['event'] == 'succeeded':
                        succeededTasks[r['id']] = r
                    else:
                        tPrint("%s - %s - %s" % (startTime, rID, r['state']['event']))
                except:
                    tPrint("Something wrong with %s" % rID)
                count = count + 1
            if stillProcessing:
                time.sleep(sleepTime)
        return ({'SUCCEEDED':succeededTasks, 'FAILED':failedTasks})