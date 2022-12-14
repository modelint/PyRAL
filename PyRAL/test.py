"""
test.py -- test the library

"""
import logging
import logging.config
import database  # creates a tcl interpreter with TclRAL package
from rtypes import Attribute
from relvar import Relvar
from pathlib import Path
from PyRAL import version

_logpath = Path("PyRAL.log")


def get_logger():
    """Initiate the logger"""
    log_conf_path = Path(__file__).parent / 'log.conf'  # Logging configuration is in this file
    logging.config.fileConfig(fname=log_conf_path, disable_existing_loggers=False)
    return logging.getLogger(__name__)  # Create a logger for this module


def test():
    logger = get_logger()
    logger.info(f'PyRAL version: {version}')
    logger.info("Starting test")
    r = Relvar(
        name = 'Domain',
        header=[
            Attribute(name='Name', type='string'),
            Attribute(name='Alias', type='string')
        ],
        identifiers=([['Name'], ['Alias']])
    )
    logger.info("No problemo")  # We didn't die on an exception, basically


if __name__ == "__main__":
    test()
