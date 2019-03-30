import logging

logger = logging.getLogger()
handler = logging.StreamHandler()
fh = logging.FileHandler('ecc.log')
formatter = logging.Formatter(
        '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
fh.setFormatter(formatter)
logger.addHandler(fh)
#logger.addHandler(handler)
logger.setLevel(logging.INFO)
