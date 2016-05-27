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
    tasks = {log: 1, getSpecificNodeObserver: 1, getSpecificNodeRoleObserver: 1, getAuxOverlord: 1,
             getAuxIntervalOverlord: 1, getCollectdOverlord: 1, getLSFOverlord: 1, getESCoreOverlord: 1, getLSCoreOverlord: 1,
             getAuxStatus: 1, getLSConf: 1, getLSCred: 1, getCoreStatus: 1, getNodeRoles: 1, queryJson: 1, queryCSV: 1}

    def on_start(self):
        getnodesObserver(self)


class DMONUser(HttpLocust):
    task_set = DICEBehavior
    min_wait = 500
    max_wait = 900