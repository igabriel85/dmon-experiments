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

from cm_api.api_client import ApiResource
import sys
from pyanomalyLogger import *


class pyDmonCMFController:
    def __init__(self, cm_host, user, passwd):
        self.cm_host = cm_host
        self.user = user
        self.passwd = passwd

    def getClusterInformation(self):
        api = ApiResource(self.cm_host, username=self.user, password=self.passwd)
        logger.info('Received; user -> %s, password -> %s, host -> %s', self.user, self.passwd, self.cm_host)
        for c in api.get_all_clusters():
            clusterInf = "Cluster name %s and version %s" %(c.name, c.version)
            #print "Cluster name %s and version %s" %(c.name, c.version)
            logger.info("Cluster name %s and version %s", c.name, c.version)
            if c.version == "CDH5":
                cdh5 = c
        return cdh5, clusterInf

    def getServiceInfo(self, serviceOrg, cluster):
        service = serviceOrg.upper()
        supportedServices = ['HDFS', 'ZOOKEEPER', 'YARN', 'SPARK_ON_YARN']
        if service not in supportedServices:
            print "Unsupported service %s" %service
            logger.error("Unsupported service %s", service)
            sys.exit("Unsupported service %s" %service)
        else:
            for s in cluster.get_all_services():
                print "Service %s (%s) found in cluster %s with state %s and health %s " %(s.type, s.name, cluster.name, s.serviceState,
                                                                               s.healthSummary)
                logger.info("Service %s (%s) found in cluster %s with state %s and health %s ", s.type, s.name, cluster.name, s.serviceState,
                                                                               s.healthSummary)
                if s.type == service:
                    print "Selected service is %s" %service
                    logger.info("Selected service is %s", service)
                    selectedService = s
        return selectedService, selectedService.name, selectedService.serviceState, selectedService.healthSummary

    def getServiceConfiguration(self, service, fdescription=False):
        print "Getting configuration for service %s" %service
        logger.info("Getting configuration for service %s", service)
        for name, config in service.get_config(view='full')[0].items():
            print "Parameter name -> %s" %name
            logger.info("Parameter name -> %s", name)
            print "Configuration -> %s" %config
            logger.info("Configuration -> %s", config)
            print "Parameter name for descriptor -> %s" %config.relatedName
            logger.info("Parameter name for descriptor -> %s", config.relatedName)
            if fdescription:
                print "Descritpion of parameter -> %s" %config.description
                logger.info("Descritpion of parameter -> %s", config.description)
            #print config.value

    def setServiceConfiguration(self, service, confiDescriptor):
        for k, v in confiDescriptor.iteritems():
            print "Desired role Type is -> %s" %k
            logger.info("Desired role Type is -> %s", k)
            for role in service.get_all_role_config_groups():
                # print role
                # print role.roleType
                if k in role.roleType:
                    print 'Updating paramerers %s for roleType %s' %(v, k)
                    logger.info('Updating paramerers %s for roleType %s', v, k)
                    role.update_config(v)
                    print 'Update for roleType %s finished' %k
                    logger.info('Update for roleType %s finished', k)
        print "Finished desired configuration changes for service %s" %service.name
        logger.info("Finished desired configuration changes for service %s", service.name)

    def restartService(self, service):
        print 'Restarting %s at %s' %(service, datetime.datetime.now())
        logger.info('Restarting %s at %s', service, datetime.datetime.now())
        cmd = service.restart().wait()
        print "%s active -> %s" %(service, cmd.active)
        logger.info("%s active -> %s", service, cmd.active)
        print "Active: %s. Success: %s" % (cmd.active, cmd.success)
        logger.info("Active: %s. Success: %s", cmd.active, cmd.success)
        if cmd.success:
            print "Restarted service %s at %s" %(service, datetime.datetime.now())
            logger.info("Restarted service %s at %s", service, datetime.datetime.now())
            return 1
        else:
            print "Restart of service %s failed at %s" %(service, datetime.datetime.now())
            logger.info("Restarted of  service failed %s at %s", service, datetime.datetime.now())
            return 0


    def restartCluster(self):
        return "Restart entire cluster"


if __name__ == '__main__':
    cm_host = ""
    user = ''
    passwd = ''
    testCMF = pyDmonCMFController(cm_host, user, passwd)
    try:
        cInstance, cInfo = testCMF.getClusterInformation()
    except Exception as inst:
        print type(inst)
        print inst.args
        sys.exit()



    print cInfo

    s, n, st, h = testCMF.getServiceInfo('HDFS', cInstance)
    print s
    print n
    print st
    print h

    conf = testCMF.getServiceConfiguration(s)

    print conf

    descriptor = {"DATANODE": {"dfs_datanode_du_reserved": "8455053312"}}

    testCMF.setServiceConfiguration(s, descriptor)

    testCMF.restartService(s)