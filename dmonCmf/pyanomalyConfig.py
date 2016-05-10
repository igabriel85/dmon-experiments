from pyanomalyLogger import *
import ConfigParser


def ConfigSectionMap(section, Config):
    dict1 = {}
    options = Config.options(section)
    for option in options:
        try:
            dict1[option] = Config.get(section, option)
        except:
            logger.warning('Exception on %s, setting default options')
            user = 'admin'
            passwd = 'admin'
            endpoint = '127.0.0.1'
            return user, passwd, endpoint

    if 'user' not in dict1:
        logger.error('User not set in conf file, setting default')
        user = 'admin'
    else:
        user = dict1['user']
    if 'passwd' not in dict1:
        logger.error('Password not set in conf file, setting default')
        passwd = 'admin'
    else:
        passwd = dict1['passwd']
    if 'endpoint' not in dict1:
        logger.error('Endpoint not set in conf file, setting default')
        endpoint = '127.0.0.1'
    else:
        endpoint = dict1['endpoint']
    return user, passwd, endpoint




def readConfDA(conf):
    logger.info('Set conf file %s', conf)
    Config = ConfigParser.ConfigParser()
    Config.read(conf)
    logger.info('Sections in conf %s', Config.sections()[0])
    user, passwd, endpoint = ConfigSectionMap(Config.sections()[0], Config)
    logger.info('Parameters; user-> %s, password-> %s, endpoint-> %s', user, passwd, endpoint)
    return user, passwd, endpoint


# u, p, e = readConfDA('daconf.ini')
#
# print u
# print p
# print e
