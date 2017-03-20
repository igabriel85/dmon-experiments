import time
import socket
import threading
from random import *
from collections import *
import sys, getopt
from dmon_load_msg import *
import numpy
import multiprocessing
MsgGenerator = DMonMSgGenerator('collectd_out.txt', 'yarn_out.txt')

MsgGenerator.readCollectdMsg()
MsgGenerator.readyarnMsg()


def median(lst):
    '''
    :param lst: list of values
    :return: median
    '''
    return numpy.median(numpy.array(lst))


def percentage(part, whole):
    '''
    :param part: part to calculate percentage
    :param whole: all messages
    :return: percentage
    '''
    return 100 * float(part)/float(whole)

def udp_worker(ip, port, mSize, thID):
    '''
    :param ip: ip of logstash
    :param port: port of logstash udp input filter
    :param mSize: size of the message (cardinality)
    :return:
    '''
    start = time.time()
    sock = socket.socket(socket.AF_INET,
            socket.SOCK_DGRAM)
    print'Begin sendding data from thread %s to port %d' % (str(thID), port)
    retval = 0
    MESSAGE_BASE = "Thread ID %s Message number %s range %s \n"
    for i in range(0, mSize):
        retval += sock.sendto(MESSAGE_BASE % (str(thID), i, randrange(100)), (ip, port))
    print('Total amount of data sent %d in time %s' % (retval, str(time.time() - start)))


def tcp_worker(ip, port, mSize, thID):
    '''
    :param ip: ip of logstash
    :param port: port of logstash tcp worker
    :param mSize: size of the message (cardinality)
    :param thID: thread id
    :return:
    '''
    start = time.time()
    sock = socket.socket(socket.AF_INET,
            socket.SOCK_STREAM)
    sock.connect((ip, port))
    print'Begin sending data from thread %s to port %d' % (str(thID), port)
    MESSAGE_BASE = "Thread ID %s Message number %s range %s \n"
    for i in range(0, mSize):
        # print "Sending message %s" %int(i)
        # sock.sendall(MESSAGE_BASE % (str(thID), i, mSize))
        sock.sendall(MsgGenerator.genCollectdMsg(str(thID), i, mSize))
        #data = sock.recv(1024)
    sock.close()
    print 'Total in time for thread %s is -> %s' %(str(thID), str(time.time() - start))


def tcp_worker2(ip, port, mSize, thID, wait=0):
    '''
    :param ip: ip of logstash
    :param port: port of logstash tcp worker
    :param mSize: size of the message (cardinality)
    :param thID: thread id
    :return:
    '''
    thread_times = []
    print'Begin sending data from thread %s to port %d' % (str(thID), port)
    for i in range(0, mSize):
        start = time.time()
        failedMsg = []
        sock = socket.socket(socket.AF_INET,
                             socket.SOCK_STREAM)
        try:
            sock.connect((ip, port))
        except:
            # print "Failed connection for thread %s message %s" %(str(thID), str(i))
            failedMsg.append(i)
            continue
        try:
            sock.sendall(MsgGenerator.genCollectdMsg(str(thID), i, mSize))
        except:
            # print "Failed msg sending for thread %s message %s" %(str(thID), str(i))
            failedMsg.append(i)
            continue
        sock.close()
        thread_times.append(time.time() - start)
        if wait:
            time.sleep(wait)
    # print thread_times
    # print sum(thread_times)
    totalTime = sum(thread_times)
    mediumTime = sum(thread_times)/len(thread_times)
    mediantime = median(thread_times)
    nrOfFailed = len(failedMsg)
    failedPercent = percentage(nrOfFailed, mSize)
    print 'Total  time for thread %s is -> %s' %(str(thID), str(totalTime))
    print 'Average event time for thread %s is -> %s' %(str(thID), str(mediumTime))
    print 'Median event time for thread %s is -> %s' %(str(thID), str(mediantime))
    print 'Failed thread %s messages: %s' %(str(thID), str(nrOfFailed))
    print 'Percentage of Failed Messages thread %s is : %s' %(str(thID), str(failedPercent))


def main(argv):
    ip = '127.0.0.1'
    port = '5999'
    PROCESS = 5
    mSize = 100
    wait = 0
    execute = 0
    try:
        opts, args = getopt.getopt(argv, "he:p:t:m:w:x:", ["endpoint=", "port=", "threads=", "mCount=", "wait=", "execute=" ])
    except getopt.GetoptError:
        print 'dmon_ls_load.py -e <endpoint> -p <port> -t <threads> -m <message_count> -w <delay between msg> -e <thread_or_process>'
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print 'dmon_ls_load.py -e <endpoint> -p <port> -t <threads> -m <message_count> -w <delay between msg> -e <thread_or_process>'
            sys.exit()
        elif opt in ('-e', '--endpoint'):
            ip = arg
        elif opt in ('-p', '--port'):
            port = arg
        elif opt in ('-t', '--threads'):
            PROCESS = int(arg)
        elif opt in ('-m', '--mCount'):
            mSize = int(arg)
        elif opt in ('-w', '--wait'):
            wait = arg
        elif opt in ('-e', '--execute'):
            execute = args

    if execute:
        workers = deque()

        for i in range(0, PROCESS):
            #t = threading.Thread(target = udp_worker, args = [ip, int(port), mSize, i])   # comments this for testing tcp only
            t = threading.Thread(target=tcp_worker2, args=[ip, int(port), mSize, i, wait])
            t.start()
            print("%s start" % t)
            workers.append(t)
        for w in workers:
            print("%s wait for join" % w)
            w.join()
            print("%s joined" % w)
    else:
        jobs = []
        for i in range(0, PROCESS):
            p = multiprocessing.Process(target=tcp_worker2, args=(ip, int(port), mSize, i, wait,))
            jobs.append(p)
            p.start()


if __name__ == '__main__':
    if len(sys.argv) == 1:
        workers = deque()
        PROCESS = 2
        mSize = 100
        wait = 0
        port = '5000'
        ip = '85.120.206.27'

        jobs = []
        for i in range(0, PROCESS):
            p = multiprocessing.Process(target=tcp_worker2, args=(ip, int(port), mSize, i, wait,))
            jobs.append(p)
            p.start()
        # for i in range(0, PROCESS):
        #     #t = threading.Thread(target = udp_worker, args = [ip, port])   # comments this for testing tcp only
        #     t = threading.Thread(target=tcp_worker2, args=[ip, int(port), mSize, i, wait])
        #     t.start()
        #     print("%s start" % t)
        #     workers.append(t)
        # for w in workers:
        #     print("%s wait for join" % w)
        #     w.join()
        #     print("%s joined" % w)

    else:
        main(sys.argv[1:])