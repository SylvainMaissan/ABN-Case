"""Creating and configuring a custom logger"""
import logging

import coloredlogs

FILE_LOG_LEVEL = logging.INFO
CONSOLE_LOG_LEVEL = logging.INFO

coloredlogs.DEFAULT_LEVEL_STYLES = {
    'info': {'color': 'white'}
}


def get_custom_logger(name) -> logging.Logger:
    """
    Create a custom logger.
    :param name: Name of the logger
    :return: Logger
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)  # Set the log level

    # Create a filehandler object
    filehandler = logging.FileHandler('app.log', mode='a')
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
