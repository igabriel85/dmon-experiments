from locust import HttpLocust, TaskSet


def log(l):
    l.client.get("/dmon/v1/log")


def getnodesObserver(l):
    l.client.get("/dmon/v1/observer/nodes")


def getSpecificNodeObserver(l):
    l.client.get("/dmon/v1/observer/nodes/dice.cdh.master")


def getSpecificNodeRoleObserver(l):
    l.client.get("/dmon/v1/observer/nodes/dice.cdh.master/roles")


def getAuxOverlord(l):
    l.client.get("/dmon/v1/overlord/aux/deploy")


def getAuxIntervalOverlord(l):
    l.client.get("/dmon/v1/overlord/aux/lsf/config")


def getCollectdOverlord(l):
    l.client.get("/dmon/v1/overlord/aux/collectd/config")


def getLSFOverlord(l):
    l.client.get("/dmon/v1/overlord/aux/lsf/config")


def getESCoreOverlord(l):
    l.client.get("/dmon/v1/overlord/core/es")


def getLSCoreOverlord(l):
    l.client.get("/dmon/v1/overlord/core/ls")


def getAgentOverlord(l):
    l.client.get("/dmon/v2/overlord/aux/agent")


def getAuxStatus(l):
    l.client.get("/dmon/v2/overlord/aux/status")


def getLSConf(l):
    l.client.get("/dmon/v1/overlord/core/ls/config")


def getLSCred(l):
    l.client.get("/dmon/v1/overlord/core/ls/credentials")


def getCoreStatus(l):
    l.client.get("/dmon/v1/overlord/core/status")


def getNodeRoles(l):
    l.client.get("/dmon/v1/overlord/nodes/roles")


def queryJson(l):
    query = {
        "DMON": {
            "fname": "output",
            "ordering": "desc",
            "queryString": "*",
            "size": 500,
            "tstart": "now-1d",
            "tstop": "None"
                }
            }
    l.client.post("/dmon/v1/observer/query/json", query)

def queryCSV(l):
    query = {
        "DMON": {
            "fname": "output",
            "ordering": "desc",
            "queryString": "*",
            "size": 500,
            "tstart": "now-1d",
            "tstop": "None"
                }
            }
    l.client.post("/dmon/v1/observer/query/csv", query)

# def login(l):
#     l.client.post("/login", {"username": "ellen_key", "password": "education"})
#
#
# def index(l):
#     l.client.get("/")
#
#
# def profile(l):
#     l.client.get("/profile")


class DICEBehavior(TaskSet):
    tasks = {getnodesObserver: 2, getSpecificNodeObserver: 1, getSpecificNodeRoleObserver: 2, getAuxOverlord: 2,
             getAuxIntervalOverlord: 1, getCollectdOverlord: 3, getLSFOverlord: 3, getESCoreOverlord: 1, getLSCoreOverlord: 1,
             getAuxStatus: 1, getLSConf: 2, getLSCred: 1, getCoreStatus: 2, getNodeRoles: 2, queryJson: 2, queryCSV: 2}

    def on_start(self):
        log(self)


class DMONUser(HttpLocust):
    task_set = DICEBehavior
    min_wait = 500
    max_wait = 900