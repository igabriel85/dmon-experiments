import logging
import datetime

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

logFile = 'exp-%s.log' %datetime.datetime.now()

handler = logging.FileHandler(logFile)
handler.setLevel(logging.INFO)
logger.addHandler(handler)