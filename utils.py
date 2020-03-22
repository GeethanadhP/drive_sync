import hashlib
import logging
import os


def get_logger(name):
    log_format = "[%(asctime)s][%(levelname)-5s]" "[%(name)s][%(funcName)s] %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_format)
    logging.getLogger("googleapiclient").setLevel(logging.ERROR)
    return logging.getLogger(os.path.basename(name))


def get_md5(path):
    hash_md5 = hashlib.md5()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(40960), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()
