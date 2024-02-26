import logging

import coloredlogs

FILE_LOG_LEVEL = logging.INFO
CONSOLE_LOG_LEVEL = logging.INFO

coloredlogs.DEFAULT_LEVEL_STYLES = {
    'info': {'color': 'white'},  # Set info level logs to white
    # Add or modify other levels as needed
}


def get_custom_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)  # Set the log level

    # Create a filehandler object
    fh = logging.FileHandler('app.log', mode='a')
    fh.setLevel(logging.INFO)

    # Create file formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    # Create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # Create console formatter
    formatter = coloredlogs.ColoredFormatter(fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    return logger
