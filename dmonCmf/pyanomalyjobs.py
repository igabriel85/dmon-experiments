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

import subprocess
import json
from pyanomalyConfig import *
from pyanomalycmf import *
import os.path
import shutil
import datetime
import sys, getopt


def moveFile(dirName):
    '''
    Creates a new directory and moves all files  to it with extension .err, .out and .log
    :param dirName: name of the directory
    '''
    if os.path.isdir(dirName):
        incr = int(dirName[-1])
        newstr = dirName[:-1]
        dirName = '%s%s' %(newstr, str(incr+1))
        moveFile(dirName)
    else:
        os.makedirs(dirName)
        wDir = os.getcwd()
        source = os.listdir(wDir)
        destination = dirName
        for files in source:
            print files
            if files.endswith('.log'):
                shutil.move(os.path.join(wDir, files), os.path.join(destination, files))
            if files.endswith('.err'):
                shutil.move(os.path.join(wDir, files), os.path.join(destination, files))
            if files.endswith('.out'):
                shutil.move(os.path.join(wDir, files), os.path.join(destination, files))




def loadJsonDescriptor(descriptor):
    '''
    :param descriptor: -> descriptor file location
    :return: -> descriptor json
    '''
    with open(descriptor) as json_descriptor:
        try:
            expData = json.load(json_descriptor)
        except:
            logger.error('File is not a valid json')
            sys.exit('Invalid descriptor')
        logger.info('Loaded experimental descriptor from %s', descriptor)
        return expData


def jobCMDConstructor(type, arguments):
    '''
    :param type: -> job type, can be spark or yarn
    :param arguments:  -> arguments of the spark or yarn job inputed as a list
    :return: -> cmd string to be executed
    '''
    if type == 'yarn':
        logger.info('Yarn job selected with arguments %s', arguments)
        bCMD = 'sudo -u hdfs hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-mapreduce-examples-2.6.0-cdh5.7.0.jar'
        bCMDList = bCMD.split()
        expCMD = bCMDList + arguments
    elif type == 'spark':
        logger.info('Spark job selected with arguments %s', arguments)
        bCMD = 'sudo -u hdfs spark-submit --files=/etc/spark/conf/metrics.properties --conf spark.metrics.conf=metrics.properties  --class org.apache.spark.examples.SparkPi --deploy-mode cluster --master yarn /opt/cloudera/parcels/CDH/lib/spark/examples/lib/spark-examples-1.3.0-cdh5.4.3-hadoop2.6.0-cdh5.4.3.jar'
        bCMDList = bCMD.split()
        expCMD = bCMDList + arguments
    else:
        expCMD = 0
        logger.error('Invalid job type')
    return expCMD


def anomalyJob(expname, jobCMD):
    '''
    :param expname: -> name of experiment, used also for creating out and err file
    :param jobCMD: -> cmd string used to run job
    :return:
    '''
    if not jobCMD:
        logger.error('Invalid job comand received, exiting')
        sys.exit('Invalid cmd received!')
    outFile = '%s.out' %(expname)
    errFile = '%s.err' %(expname)

    with open(outFile, 'wb') as out, open(errFile, "wb") as err:
        try:
            logger.info('Started job %s', expname)
            job = subprocess.Popen(jobCMD, stdout=out, stderr=err).wait()
        except Exception as inst:
            logger.info('An Exception has occured while running job %s. Exception type %s with arguments %s!'
                        %(expname, type(inst), inst.args))


def main(argv):
    try:
        opts, args = getopt.getopt(argv, "hd:", ["descriptor="])
    except getopt.GetoptError:
        print "pyanomalyjobs.py -d <descriptor>"
        sys.exit(2)
    for opt, arg in opts:
        if opt == "-h":
            print "This script runs Yarn and Spark example jobs sequencially"
            print "Correct usage is: "
            print "pyanomalyjobs.py -d <descriptor>"
            sys.exit()
        elif opt in ("-d", "--descriptor"):
            des = arg
            if not os.path.isfile(des):
                print "Descriptor %s not found" %(des)
            if not os.path.isfile('daconf.ini'):
                print "CMF Conf file not found, using default values"
                user = 'admin'
                passwd = 'admin'
                endpoint = ' 127.0.0.1'
            else:
                user, passwd, endpoint = readConfDA('daconf.ini')

            pyaCNF = pyDmonCMFController(str(endpoint), user, passwd)

            des_data = loadJsonDescriptor(des)
            print "Started jobs at %s" %(datetime.datetime.now())
            logger.info("Started jobs at %s", datetime.datetime.now())
            #print des_data.keys()
            for k, v in des_data.iteritems():
                #print k
                print "Started experiment  %s at %s" %(k, datetime.datetime.now())
                logger.info("Started experiment  %s at %s", k, datetime.datetime.now())
                counter = 0
                for e in v:
                    counter += 1
                    # Introduce service config here
                    if 'conf' in e:
                        try:
                            cdh5, cdhInfo = pyaCNF.getClusterInformation()
                        except Exception as inst:
                            logger.error('Error connecting to CMF with %s and %s', type(inst), inst.args)
                            sys.exit('Error connecting to CMF')
                        setService = e['conf']
                        for sk, sv in setService.iteritems():
                            print 'Selecting service %s for parameter changes' %sk
                            logger.info('Selecting service %s for parameter changes', sk)
                            serviceObj, n, st, h = pyaCNF.getServiceInfo(sk, cdh5)
                            logger.info('Changing service %s  parameter %s', sk, sv)
                            pyaCNF.setServiceConfiguration(serviceObj, sv)
                            rStatus = pyaCNF.restartService(serviceObj)
                            print rStatus
                            if not rStatus:
                                logger.error('Error while starting service %s, exiting', sk)
                                sys.exit('Error while starting service' +sk +'exiting')
                            print 'Started service %s' %sk
                            logger.info('Started service %s', sk)
                    for i in range(0, e['cardinality']):
                        print "Started iteration %s for job %s from experiment %s at %s" %(i, e, k, datetime.datetime.now())
                        logger.info("Started iteration %s for job %s from experiment %s at %s", i, e, k, datetime.datetime.now())
                        expname = 'exp-%s-%s-%s'%(k, i, counter)
                        if 'yarn' in e:
                            jType = 'yarn'
                            jTypeArg = e['yarn']
                        elif 'spark' in e:
                            jType = 'spark'
                            jTypeArg = e['spark']
                        else:
                            logger.error('Invalid job type in exp. descriptor. Exiting!')
                            sys.exit('Invalid job type in exp.')
                        #print jType
                        jCMD = jobCMDConstructor(jType, jTypeArg)
                        #print jCMD
                        anomalyJob(expname, jCMD)
                        #print e['yarn']
                        #print expname
                        print "Finished iteration %s for job %s from experiment %s at %s" %(i, e, k, datetime.datetime.now())
                        logger.info("Finished iteration %s for job %s from experiment %s at %s", i, e, k, datetime.datetime.now())
                print "Finished experiment %s at %s" %(k, datetime.datetime.now())
                logger.info("Finished experiment %s at %s", k, datetime.datetime.now())
            destFolder = 'exp-1'
            moveFile(destFolder)
            print 'Moved exp files to folder'
            logger.info('Moved exp files to folder')

if __name__ == '__main__':
    main(sys.argv[1:])


