"""

Copyright 2016, Institute e-Austria, Timisoara, Romania
    http://www.ieat.ro/
Developers:
 * Gabriel Iuhasz, iuhasz.gabriel@info.uvt.ro

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at:
    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import requests


def getYarnInformation(yhIP, yhPort):
    rhURL = 'http://%s:%s/ws/v1/history' %(yhIP, str(yhPort))
    print rhURL
    rhiURL = 'http://%s:%s/ws/v1/history/info' %(yhIP, str(yhPort))
    print rhiURL
    try:
        rh = requests.get(rhURL)
    except Exception as inst:
        print "Exception %s with %s while connecting" %(type(inst), inst.args)
        return 0

    try:
        rhi = requests.get(rhiURL)
    except Exception as inst:
        print "Exception %s with %s while connecting" %(type(inst), inst.args)
        return 0
    # print rh.status_code
    # print rh.json()
    return rh.status_code, rh.json(), rhi.status_code, rhi.json()


def getYarnJobs(yhIP, yhPort):
    jURL = 'http://%s:%s/ws/v1/history/mapreduce/jobs' %(yhIP, str(yhPort))
    try:
        rJobs = requests.get(jURL)
    except Exception as inst:
        print "Exception %s with %s while getting jobs" %(type(inst), inst.args)
        return 0
    return rJobs.status_code, rJobs.json()

def getYarnJobsStatistic(yhIP, yhPort, jDescriptor):
    if jDescriptor['jobs'] is None:
        print "No jobs found"
        return 0
    jList = []
    print len(jDescriptor['jobs']['job'])
    for j in jDescriptor['jobs']['job']:
        jList.append(j['id'])

    responseList = []
    for id in jList:
        jURL = 'http://%s:%s/ws/v1/history/mapreduce/jobs/%s' %(yhIP, str(yhPort), id)
        try:
            rJobId = requests.get(jURL)
        except Exception as inst:
            print "Exception %s with %s while getting job details" %(type(inst), inst.args)
            return 0
        responseList.append(rJobId.json())
    retDict = {}
    retDict['jobs'] = responseList

    return retDict


def getYarnJobTasks(yhIP, yhPort, jDescriptor):
    if jDescriptor['jobs'] is None:
        print "No jobs found"
        return 0
    jList = []
    for j in jDescriptor['jobs']['job']:
        jList.append(j['id'])

    responseList = []
    for id in jList:
        jURL = 'http://%s:%s/ws/v1/history/mapreduce/jobs/%s/tasks' %(yhIP, str(yhPort), id)
        try:
            rJobId = requests.get(jURL)
        except Exception as inst:
            print "Exception %s with %s while getting job details" %(type(inst), inst.args)
            return 0
        data = rJobId.json()
        data['jobId'] = id
        responseList.append(data)
    retDict = {}
    retDict['jobs'] = responseList

    return retDict

if __name__ == '__main__':
    hServer = "85.120.206.40"
    hPort = 19888

    historyCode, historyResponse, historyInfoCode, historyInfoResponse = getYarnInformation(hServer, hPort)


    # print historyCode
    # print historyResponse
    # print historyInfoCode
    # print historyInfoResponse

    jStatus, jResponse = getYarnJobs(hServer, hPort)

    #print jStatus
    print jResponse

    # for j in jResponse['jobs']['job']:
    #     print j['id']

    resp = getYarnJobsStatistic(hServer, hPort, jResponse)
    print resp

    respTask = getYarnJobTasks(hServer, hPort, jResponse)
    print respTask