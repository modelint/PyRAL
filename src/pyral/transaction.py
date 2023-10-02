"""
transaction.py -- Database transaction
"""
import logging
from pyral.exceptions import (DuplicateTransaction, NoOpenTransaction, UnNamedTransaction, PyRALException,
                              SessionNotOpen)
from pyral.database import Database

_logger = logging.getLogger(__name__)

class Transaction:
    """
    A TclRAL transaction

    """
    pending = {}

    @classmethod
    def open(cls, db: str, name: str) -> str:
        """
        Starts a new empty transaction by ensuring that there are no statements

        :param db: The DB session
        :param name: The name of this transaction, must be unique for the db
        :return: The provided name is returned to make it easy for client to use downstream
        """
        # Verify that the name is not an empty string
        if not name:
            _logger.error(f"No transaction name provided on open for db [{db}]")
            raise UnNamedTransaction

        # Add the database to the pending dictionary if it isn't already registered
        if db not in cls.pending:
            if db not in Database.sessions:
                _logger.error(f"No open session for database [{db}]")
                raise SessionNotOpen
            cls.pending[db] = {}
        elif name in db:
            # Verify that the transaction is not already open
            _logger.error(f"Transaction {name} already pending for db [{db}]")
            raise DuplicateTransaction

        # Create a new empty tranaction
        cls.pending[db][name] = []
        _logger.info(f"OPEN > {db}:{name}")
        return name

    @classmethod
    def append_statement(cls, db: str, name: str, statement: str):
        """
        Adds a statement to the list of pending statements in the open transaction.

        :param db: The DB session
        :param name: The name of the transaction
        :param statement:  Statement to be appended
        """
        # Ensure that the statement is not empty
        if not statement:
            _logger.error(f"No statement provided on append to transaction [{name}] for db [{db}]")
            raise PyRALException

        # Verify that the transaction is pending
        if db not in cls.pending:
            _logger.error(f"No open db session: [{db}]")
            raise PyRALException

        # Append the statement to the pending transaction
        try:
            cls.pending[db][name].append(statement)
        except KeyError:
            _logger.error(f"No transaction [{name}] open on db [{db}]")
            raise NoOpenTransaction


    @classmethod
    def execute(cls, db: str, name: str):
        """
        Executes all statements in the specified transaction as a TclRAL relvar eval transaction

        :param db: The DB session
        :param name: The name of the transaction
        :return:  The TclRal success/fail result
        """
        try:
            cmd = f"relvar eval " + "{\n    " + '\n    '.join(cls.pending[db][name]) + "\n}"
        except KeyError:
            _logger.error(f"No transaction [{name}] open on db [{db}]")
            raise NoOpenTransaction

        _logger.info(f"Executing transaction:")
        _logger.info(cmd)
        result = Database.sessions[db].eval(cmd)

        # Delete the executed transaction
        del cls.pending[db][name]

        _logger.info(f"EXECUTED > {db}:{name}")
        _logger.info(f"With result: [{result}]")
