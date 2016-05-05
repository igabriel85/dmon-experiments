import socket
import logging
import datetime
import time

UDP_IP = "127.0.0.1"
UDP_PORT = 5005
sock = socket.socket(socket.AF_INET,  # Internet
                        socket.SOCK_DGRAM)  # UDP
sock.bind((UDP_IP, UDP_PORT))
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler = logging.FileHandler('PerfExpListener.log')
handler.setLevel(logging.INFO)

logger.addHandler(handler)


if __name__ == '__main__':
    while True:
        data, addr = sock.recvfrom(1024)  # buffer size is 1024 bytes
        #print "received message:", data
        logger.info('[%s] Message : %s', datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'),data)


