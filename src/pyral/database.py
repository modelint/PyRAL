"""
database.py - Create and manage a set of tclRAL databases
"""

import logging.config
from tkinter import Tcl, Tk, TclError
from pyral.exceptions import PyRALException, TclRALException
from pathlib import Path
from typing import Any

_logger = logging.getLogger(__name__)


class Database:
    """
    Manage a set of PyRAL sessions.

    A PyRAL session is simply a tcl interpreter object that has been initialized
    by importing Andrew Mangogna's TclRAL packages.

    These packages must have been compiled for your platform and placed in the directory
    assigned to the ral_lib_path below. Consult Andrew's Model Realization repository for
    the latest files.  The most recent link should appear in the github wiki for the PyRAL
    repository or in the readme.

    A PyRAL session can then be operated on with PyRAL commands that ultimately resolve into
    TclRAL commands evalued by the associated tcl interpreter object.

    A unique human-readable name is associated with each PyRAL session so that it can be easily
    managed in a dictionary.
    """
    # Path to the TclRAL library
    ral_lib_path = Path(__file__).parent / "tcl_scripts" / "init_TclRAL.tcl"
    sessions = {} # A dictionary of open TclRAL sessions keyed by session name
    # Temporary relational variable names keyed by session name
    # Key is the name of the owner and value is the variable name
    rv_names : dict[str, dict[str, set[str]]] = {}

    @classmethod
    def get_open_sessions(cls) -> set[str]:
        """
        Return the tcl interpreter dictionary representing all open database sessions.
        The purpose is mostly for diagnostics so that we can verify that there aren't any unneeded
        db sessions still hanging around after a procedure in the client completes.

        :param db: Name of the database session
        :return: Set of open database session names
        """
        return {k for k in cls.sessions.keys()}

    @classmethod
    def get_rv_names(cls, db: str) -> dict[str, set[str]]:
        """
        Return the relational variable names dictionary for a given database session.
        The purpose is mostly for diagnostics so that we can verify that there aren't any unneeded
        relation variables still hanging around after a procedure in the client completes.

        :param db: Name of the database session
        :return: Dictionary mapping owner -> set of RV names
        """
        # We copy the value since when we do diagnostics we might want to capture a before/after pair of values
        # and without the shallow copy(), both before and after will point to the same possibly mutated value.
        return cls.rv_names.get(db, {}).copy()

    @classmethod
    def open_session(cls, name: str) -> Tk:
        """
        Open a PyRAL session.

        We do this by getting a new tcl interpreter and having it load TclRAL.

        Then we add the TclRAL'd tcl interpreter object to our dictionary of open sessions.

        We return the newly initiated tcl interpreter object though the client can more conveniently
        specify its session via the name.

        :param name:
        :return:  The TclRAL tcl interpreter instance is returned
        """
        # Verify that the name is not an empty string
        if not name:
            _logger.error(f"Empty '' name provided for PyRAL session")
            raise PyRALException

        # Verify that this session is not already open
        if name in cls.sessions:
            _logger.error(f"PyRAL session [{name}] already open")
            raise PyRALException

        tcl_int = Tcl()  # Get a new tcl interpreter
        tcl_int.eval(f"source {cls.ral_lib_path}")  # Load TclRAL library
        cls.sessions[name] = tcl_int  # Add it to the open session dictionary
        cls.rv_names[name] = {}  # Initialize an empty relational variable name dict for the session
        _logger.info(f"PyRAL session [{name}] initiated")

        return tcl_int

    @classmethod
    def close_session(cls, name: str):
        """
        Closes an open TclRAL session and removes any administrative data for that session

        :param name:  Session name
        """

        # Verify that this session is open
        if name not in cls.sessions:
            _logger.error(f"PyRAL session [{name}] is not open")
            raise PyRALException

        # Clear out any administrative entries related to this session
        # from class variables
        cls.sessions.pop(name, None)  # Open database session name and interpreter
        cls.rv_names.pop(name, None)  # All tracked relation variable names

        _logger.info(f"PyRAL session [{name}] closed")

    @classmethod
    def save(cls, db: str, fname: str):
        """
        Save the db in the supplied file

        :param db:  Database name
        :param fname: File name
        """
        try:
            cls.sessions[db].eval(f"serializeToFile {fname}")
        except KeyError:
            _logger.error(f"Session [{db}] not open")
            raise PyRALException

    @classmethod
    def load(cls, db: str, fname: str):
        """
        Load the db from the supplied file

        :param db:  Database name
        :param fname: File name
        """
        try:
            cls.sessions[db].eval(f"deserializeFromFile {fname}")
        except KeyError:
            _logger.error(f"Session [{db}] not open")
            raise PyRALException

    @classmethod
    def execute(cls, db: str, cmd: str, log: bool = True) -> str:
        """
        Executes a TclRAL command via the supplied session and returns TclRAL's string result.

        :param db: The DB session
        :param cmd: A TclRAL command string
        :param log:  If false, the result will not be logged. Useful when no meaningful result is expected
        :return: The string received as a result of executing the command
        """
        _logger.info(f"cmd: {cmd}")
        try:
            result = cls.sessions[db].eval(cmd)
        except KeyError:
            _logger.error(f"Database {db} is not open")
            raise PyRALException
        except TclError as e:
            _logger.error(f"TclRAL error in db [{db}] on command: [{cmd}]")
            _logger.exception(e)
            raise TclRALException

        if log:
            _logger.info(f"result: {result}")
        return result

    @classmethod
    def names(cls, db: str, pattern: str = "") -> str:
        """
        Use this to obtain names of all created relvars using the optional pattern.

        :param db:  Database name
        :param pattern:  Apply this optional pattern
        :return: TclRAl returned string
        """
        try:
            result = cls.sessions[db].eval(f"relvar names {pattern}")
        except KeyError:
            _logger.error(f"Session [{db}] not open")
            raise PyRALException

        _logger.info(f"Names in sesssion [{db}] using pattern [{pattern}]")
        _logger.info(result)
        return result

    @classmethod
    def constraint_names(cls, db: str, pattern: str = ""):
        """
        Use this to obtain names of all created constraints and names using the optional pattern.

        :param db:  Database name
        :param pattern:  Apply this optional pattern
        """
        try:
            result = cls.sessions[db].eval(f"relvar constraint names {pattern}")
        except KeyError:
            _logger.error(f"Session [{db}] not open")
            raise PyRALException

        _logger.info(f"Constraints and names in sesssion [{db}] using pattern [{pattern}]")
        _logger.info(result)
        return result
