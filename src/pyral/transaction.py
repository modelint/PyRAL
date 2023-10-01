"""
transaction.py -- Database transaction
"""
import logging
from pyral.exceptions import IncompleteTransactionPending, NoOpenTransaction, UnNamedTransaction
from tkinter import Tk

_logger = logging.getLogger(__name__)

class Transaction:
    """
    A TclRAL transaction

    """
    pending = {}
    _statements = None
    _cmd = ""
    _result = None
    _schema = []
    _tclral = None

    # def __init__(self, tclral: Tk, name: str):
    #     self.name = name
    #     self.tclral = tclral
    #     self.statements = []
    #
    #     if not name:
    #         _logger.error(f"Cannot open transaction with an empty string name")
    #         raise UnNamedTransaction
    #
    #     if name in Transaction.pending:
    #
    #
    #     _logger.info(f"PYRAL TR [{name}] OPEN")
    #
    # def open(self):
    #

    @classmethod
    def open(cls, tclral: Tk, name: str):
        """
        Starts a new empty transaction by ensuring that there are no statements
        """
        # TODO: As it stands, only one Transaction is open for all potential tclral instances.
        # TODO: We should make the class methods instance based and tie each instance of Transaction to a single
        # TODO: TclRAL session. (For now it works since we aren't yet opening multiple TclRAL sessions simultaneously.
        _logger.info(f"PYRAL TR OPEN")
        cls._tclral = tclral
        if cls._statements:
            _logger.error(f"New transaction opened before closing previous.")
            raise IncompleteTransactionPending
        cls._statements = []

    @classmethod
    def append_statement(cls, statement: str):
        """
        Adds a statement to the list of pending statements in the open transaction.

        :param statement:  Statement to be appended
        """
        if not isinstance(cls._statements, list):
            _logger.exception("Statement append when no transaction is open.")
            raise NoOpenTransaction
        cls._statements.append(statement)

    @classmethod
    def execute(cls):
        """
        Executes all statements as a TclRAL relvar eval transaction
        :return:  The TclRal success/fail result
        """
        cls._cmd = f"relvar eval " + "{\n    " + '\n    '.join(cls._statements) + "\n}"
        _logger.info(f"Executing transaction:")
        _logger.info(cls._cmd)
        cls._result = cls._tclral.eval(cls._cmd)
        cls._statements = None  # The statements have been executed
        _logger.info(f"With result: [{cls._result}]")
        _logger.info(f"PYRAL TR CLOSED")

