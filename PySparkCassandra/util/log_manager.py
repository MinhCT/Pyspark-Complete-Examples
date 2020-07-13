import logging

logging.basicConfig(format='[%(asctime)s] - %(name)s - %(levelname)s: %(message)s', level=logging.INFO)


def get_logger(name="FCM-Token-Mapper"):
    log = logging.getLogger(name)
    return log
