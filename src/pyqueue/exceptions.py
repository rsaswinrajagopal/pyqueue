# import logging
# from logging.handlers import RotatingFileHandler


# def setup_logger(name: str, log_file: str, level=logging.INFO):
#     formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
#     handler = RotatingFileHandler(log_file, maxBytes=10 * 1024 * 1024, backupCount=5)
#     handler.setFormatter(formatter)

#     logger = logging.getLogger(name)
#     logger.setLevel(level)
#     logger.addHandler(handler)
#     return logger
