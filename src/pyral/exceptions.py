"""
exceptions.py – PyRAL exceptions
"""

class PyRALException(Exception):
    """ Top level PyRAL exception """
    pass

class Transaction(PyRALException):
    """ Top level PyRAL Transaction exception """
    pass


class SessionNotOpen(PyRALException):
    """ The database session has not been opened """
    pass

class TclRALException(PyRALException):
    """ A TclRAL or Tcl error occured inside the tcl interpeter """
    pass

class DuplicateTransaction(Transaction):
    """ Cannot use name to open a new transaction since one is aready open by that name """
    pass

class UnNamedTransaction(Transaction):
    """ Name does not correspond to any pending transaction """
    pass

class NoOpenTransaction(PyRALException):
    """ Attempt to add statement when no transaction has been opened """
    pass

class RestrictOneOnZeroCardinality(PyRALException):
    """ Attempted restrict-one operation on relation with no tuples """
    pass
