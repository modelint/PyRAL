"""
setops_play.py -- Play around with set operations

"""
# System
from collections import namedtuple

# PyRAL
from pyral.database import Database
from pyral.relvar import Relvar
from pyral.relation import Relation
from pyral.rtypes import Attribute


sdb = "sdb"  # Set operation db example

Element_i = namedtuple('Element_i', 'Name')

class SetOp:
    """
    SetOp examples
    """

    @classmethod
    def setup(cls):
        """
        Initialize the examples
        """
        Database.open_session(name=sdb)
        Relation.create(db=sdb, attrs=[Attribute(name="Name", type="string")],
                        tuples=[
                            Element_i(Name="A"),
                            Element_i(Name="B"),
                            Element_i(Name="C"),
                        ], svar_name="set1")
        Relation.create(db=sdb, attrs=[Attribute(name="Name", type="string")],
                        tuples=[
                            Element_i(Name="B"),
                            Element_i(Name="C"),
                        ], svar_name="set2")