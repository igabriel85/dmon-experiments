import os
import json
import random


class DMonMSgGenerator:

    collectd_msg = []
    yarn_msg = []

    def __init__(self, collectd_in, yarn_in):
        self.collectd_in = collectd_in
        self.yarn_in = yarn_in

    def readCollectdMsg(self):
        with open(self.collectd_in) as f:
            content = f.readlines()
        for event in content:
            DMonMSgGenerator.collectd_msg.append(json.loads(event))
        return DMonMSgGenerator.collectd_msg

    def readyarnMsg(self):
        with open(self.yarn_in) as f:
            content = f.readlines()
        for event in content:
            jevent = json.loads(event)
            msg = jevent['message']
            DMonMSgGenerator.yarn_msg.append(msg)
        return DMonMSgGenerator.yarn_msg

    def genCollectdMsg(self, thID, msgNR, mrange):
        '''
        :param thID: ID of active thread
        :param msgNR: number of message
        :param mrange: range
        :return: message
        '''
        msg = random.choice(DMonMSgGenerator.collectd_msg)
        msg['Thread_ID'] = thID
        msg['Msg_nr'] = msgNR
        msg['Range'] = mrange
        return str(msg)

    def genYarnMsg(self, thID, msgNr, mrange):
        '''
        :param thID: ID of active thread
        :param msgNR: number of message
        :param mrange: range
        :return:
        '''
        msg = random.choice(DMonMSgGenerator.yarn_msg)
        threadString = 'Thread_ID=%s' %str(thID)
        msgNrString = 'Msg_nr=%s' %str(msgNr)
        msgRangeString = 'Range=%s' %str(mrange)
        genMessage = ', '.join((msg, threadString, msgNrString, msgRangeString))
        print genMessage

    def dummyMsg(self, thID, msgNR, mrange):
        return "Thread ID %s Message number %s range %s \n" % (str(thID), str(msgNR), str(mrange))

# test = DMonMSgGenerator('collectd_out.txt', 'yarn.out')
#
# readTest = test.readCollectdMsg()
# test.readyarnMsg()
#
# print test.genCollectdMsg(1, 99, 100)
# print test.dummyMsg(1, 99, 100)
#
# print test.genYarnMsg(1, 99, 100)


