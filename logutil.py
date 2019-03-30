import logging
import os
from logging.handlers import TimedRotatingFileHandler

logger = logging.getLogger()
handler = logging.StreamHandler()
fh = logging.FileHandler('ecc.log')
prod_log_file_path = '/ecc_log/ecc.log'

if os.path.isfile(prod_log_file_path):
    fh = TimedRotatingFileHandler(prod_log_file_path,
                                  when="d",
                                  interval=1,
                                  backupCount=14)

formatter = logging.Formatter(
        '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
fh.setFormatter(formatter)
logger.addHandler(fh)
#logger.addHandler(handler)
logger.setLevel(logging.DEBUG)