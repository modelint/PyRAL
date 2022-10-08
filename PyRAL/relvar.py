"""
relvar.py -- Proxy for a TclRAL relvar
"""

from database import db
from typing import List
from rtypes import Attribute


class Relvar:
    """
    Proxy for a TclRAL relvar

    """

    def __init__(self, name: str, header: List[Attribute], identifiers: List[str]):
        """

        :param name:
        :param header:
        """
        self.name = name
        self.header = header
        self.identifiers = identifiers

        self.create()

    def create(self):
        """
        Create in TclRAL
        :return:
        """
        pass
        # statement = f"relvar create {self.name} '{}'"

