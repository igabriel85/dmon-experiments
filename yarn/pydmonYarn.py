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
    jList = []
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

if __name__ == '__main__':
    hServer = "85.120.206.35"
    hPort = 19888

    historyCode, historyResponse, historyInfoCode, historyInfoResponse = getYarnInformation(hServer, hPort)


    print historyCode
    print historyResponse
    print historyInfoCode
    print historyInfoResponse

    jStatus, jResponse = getYarnJobs(hServer, hPort)


    print jStatus
    print jResponse

    # for j in jResponse['jobs']['job']:
    #     print j['id']

    resp = getYarnJobsStatistic(hServer, hPort, jResponse)
    print resp