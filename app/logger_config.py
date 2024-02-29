"""Creating and configuring a custom logger"""
import functools
import logging
from logging.handlers import RotatingFileHandler

import coloredlogs

FILE_LOG_LEVEL = logging.INFO
CONSOLE_LOG_LEVEL = logging.INFO

coloredlogs.DEFAULT_LEVEL_STYLES = {
    'info': {'color': 'white'}
}


def log_dataframe_metadata(_func=None, *, logger_name="Data Cleaning"):
    """
    The decorator that logs the shape and columns of a dataframe after a specific step.
    """

    def log_decorator(func):
        @functools.wraps(func)
        def wrapper_log_transform(*args, **kwargs):
            logger = get_custom_logger(logger_name)
            args_repr = [repr(a) for a in args]
            kwargs_repr = [f"{k}={v!r}" for k, v in kwargs.items()]
            signature = ", ".join(args_repr + kwargs_repr)
            logger.info("Calling %s with args: %s", func.__name__, signature)
            dataframe = func(*args, **kwargs)

            logger.info("Dataframe shape after %s: (%d, %d)", func.__name__, dataframe.count(),
                        len(dataframe.columns))
            logger.debug("Dataframe columns after %s: %s", func.__name__, dataframe.columns)
            return dataframe

        return wrapper_log_transform

    if _func is None:
        return log_decorator
    else:
        return log_decorator(_func)


def get_custom_logger(name) -> logging.Logger:
    """
    Create a custom logger.
    :param name: Name of the logger
    :return: Logger
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)  # Set the log level

    if logger.handlers:
        return logger

    # Create a filehandler object
    filehandler = RotatingFileHandler('app.log', maxBytes=4096, backupCount=1)
    filehandler.setLevel(logging.INFO)

    # Create file formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    filehandler.setFormatter(formatter)
    logger.addHandler(filehandler)

    # Create console handler and set level to debug
    consolehandler = logging.StreamHandler()
    consolehandler.setLevel(logging.DEBUG)

    # Create console formatter
    formatter = (coloredlogs
                 .ColoredFormatter(fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    consolehandler.setFormatter(formatter)
    logger.addHandler(consolehandler)

    return logger
