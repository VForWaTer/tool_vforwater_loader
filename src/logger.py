import logging
import os

# create a new logger
logger = logging.getLogger('vforwater')

# set the log level to debug
logger.setLevel(logging.DEBUG)

# create a file handler for all messages
processing_handler = logging.FileHandler('/out/processing.log')

# create a file handler for warnings and errors
error_handler = logging.FileHandler('/out/errors.log')
error_handler.setLevel(logging.WARNING)

# create a console handler for all messages
console_handler = logging.StreamHandler()
console_handler.setLevel(getattr(logging, os.environ.get('LOG_LEVEL', 'INFO')))

# create a formatter for the file handlers
formatter = logging.Formatter('[%(asctime)s] - [%(levelname)s] - [%(message)s]')

# create a formatter to the console
console_formatter = logging.Formatter('[%(levelname)s]: %(message)s')

# set the formatters
processing_handler.setFormatter(formatter)
error_handler.setFormatter(formatter)
console_handler.setFormatter(console_formatter)

# add the handlers to the logger
logger.addHandler(processing_handler)
logger.addHandler(error_handler)
logger.addHandler(console_handler)