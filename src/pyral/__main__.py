"""
PyRAL

"""
import logging
import logging.config
import sys
import argparse
from pathlib import Path
from pyral import version

_logpath = Path("pyral.log")


def get_logger():
    """Initiate the logger"""
    log_conf_path = Path(__file__).parent / 'log.conf'  # Logging configuration is in this file
    logging.config.fileConfig(fname=log_conf_path, disable_existing_loggers=False)
    return logging.getLogger(__name__)  # Create a logger for this module


# Configure the expected parameters and actions for the argparse module
def parse(cl_input):
    parser = argparse.ArgumentParser(description='PyRAL')
    parser.add_argument('-T', '--test', action='store_true',
                        help='Run print test'),
    parser.add_argument('-R', '--rebuild', action='store_true',
                        help='Rebuild the database.'),
    parser.add_argument('-D', '--debug', action='store_true',
                        help='Debug mode'),
    parser.add_argument('-V', '--version', action='store_true',
                        help='Print the current version of PyRAL')
    return parser.parse_args(cl_input)


def main():
    # Start logging
    logger = get_logger()
    logger.info(f'PyRAL version: {version}')

    # Parse the command line args
    args = parse(sys.argv[1:])

    if args.version:
        # Just print the version and quit
        print(f'PyRAL version: {version}')

    if args.test:
        from print_table import TableTest
        TableTest.print_table()


    logger.info("No problemo")  # We didn't die on an exception, basically
    print("\nNo problemo")


if __name__ == "__main__":
    main()
