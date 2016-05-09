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
import logging
import os.path
import datetime
import sys, getopt

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

logFile = 'exp-%s.log' %datetime.datetime.now()

handler = logging.FileHandler(logFile)
handler.setLevel(logging.INFO)
logger.addHandler(handler)


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
                print "Descriptor %s not a file" %(des)
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
                    #print e
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


if __name__ == '__main__':
    main(sys.argv[1:])


