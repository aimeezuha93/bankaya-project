import logging

logging.basicConfig()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bankaya")
logger.setLevel(logging.INFO)


def get_logger():
    return logger
