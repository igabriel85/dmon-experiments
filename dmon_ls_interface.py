import requests
import sys
import time
import json


def getLogstash(ip, port, info, stats=0):
    if stats:
        resourceURL = 'http://%s:%s/_node/stats/%s'%(str(ip), str(port), info)
    else:
        resourceURL = 'http://%s:%s/_node/%s'%(str(ip), str(port), info)
    try:
        r = requests.get(resourceURL)
    except:
        print "Failed GET Request for %s" %resourceURL
        return 0
    return r.json()


def checkLS(ip, port):
    resourceURL = 'http://%s:%s' %(str(ip), str(port))
    try:
        r = requests.get(resourceURL)
        # print r.status_code
        # print r.json()
    except:
        return False
    return True


def monitorLS(period, ip, port):
    infoNode = ['pipeline', 'os', 'jvm', 'hot_threads']
    infoStats = ['jvm', 'process', 'pipeline', 'reloads', 'os']
    responseNodes = []
    responseStatus = []
    LSExp = {}
    print "Starting monitor loop"
    while checkLS(ip, port):
        for e in infoNode:
            rn = getLogstash(ip, port, e)
            if not rn:
                print "Detected potential shutdown of Logstash ..."
                continue
            else:
                responseNodes.append(rn)
        for el in infoStats:
            rs = getLogstash(ip, port, el, stats=1)
            if not rs:
                print "Detected potential shutdown of Logstash ..."
                continue
            else:
                responseStatus.append(rs)
        time.sleep(period)
    LSExp['node'] = responseNodes
    LSExp['status'] = responseStatus
    print "Logstash stopped, writing to file ..."
    if len(responseStatus) == 0 or len(responseNodes) == 0:
        print "Nothing to write!"
        sys.exit(1)
    else:
        with open('exp.json', 'w') as outfile:
            json.dump(LSExp, outfile)
        print "Writing finished!"
        sys.exit(0)


LS_IP = '85.120.206.27'
LS_PORT = '9600'
period = 5
# print checkLS(LS_IP, LS_PORT)

monitorLS(period, LS_IP, LS_PORT)

