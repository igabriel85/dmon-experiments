import time
import socket
import threading
from random import *
from collections import *
import sys, getopt

def udp_worker(ip, port, mSize):
    '''
    :param ip: ip of logstash
    :param port: port of logstash udp input filter
    :param mSize: size of the message (cardinality)
    :return:
    '''
    start = time.time()
    sock = socket.socket(socket.AF_INET,
            socket.SOCK_DGRAM)
    print('Begin sendding data to port %d' % port)
    retval = 0
    MESSAGE_BASE = 'Hello World'
    for i in range(0, mSize):
        retval += sock.sendto(MESSAGE_BASE % (i, port, randrange(100)), (ip, port))
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
    print'Begin sendding data from thread %s to port %d' % (str(thID), port)
    MESSAGE_BASE = "Thread ID %s Message number %s range %s \n"
    for i in range(0, mSize):
        sock.sendall(MESSAGE_BASE % (str(thID), i, randrange(100)))
        #data = sock.recv(1024)
    sock.close()
    print 'Total in time for thread %s is -> %s' %(str(thID), str(time.time() - start))

def main(argv):
    ip = '127.0.0.1'
    port = '5999'
    PROCESS = 20
    mSize = 10
    try:
        opts, args = getopt.getopt(argv, "he:p:t:m:", ["endpoint=", "port=", "threads=", "mCount="])
    except getopt.GetoptError:
        print 'dmon_ls_load.py -e <endpoint> -p <port> -t <thrreads'
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print 'dmon_ls_load.py -e <endpoint> -p <port>'
            sys.exit()
        elif opt in ('-e', '--endpoint'):
            ip = arg
        elif opt in ('-p', '--port'):
            port = arg
        elif opt in ('-t', '--threads'):
            PROCESS = int(arg)
        elif opt in ('-m', '--mCount'):
            mSize = int(arg)

    workers = deque()

    for i in range(0, PROCESS):
        #t = threading.Thread(target = udp_worker, args = [ip, port])   # comments this for testing tcp only
        t = threading.Thread(target=tcp_worker, args=[ip, int(port), mSize, i])
        t.start()
        print("%s start" % t)
        workers.append(t)
    for w in workers:
        print("%s wait for join" % w)
        w.join()


if __name__ == '__main__':
    if len(sys.argv) == 1:
        workers = deque()
        PROCESS = 20
        mSize = 10

        for i in range(0, PROCESS):
            port = '5999'
            ip = '127.0.0.1'
            #t = threading.Thread(target = udp_worker, args = [ip, port])   # comments this for testing tcp only
            t = threading.Thread(target=tcp_worker, args=[ip, int(port), mSize, i])
            t.start()
            print("%s start" % t)
            workers.append(t)
        for w in workers:
            print("%s wait for join" % w)
            w.join()
    else:
        main(sys.argv[1:])