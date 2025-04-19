"""
div_play.py -- Play around with division

"""
# System
from collections import namedtuple

# PyRAL
from pyral.database import Database
from pyral.relvar import Relvar
from pyral.relation import Relation
from pyral.transaction import Transaction
from pyral.rtypes import Attribute


Flow_Dependency_i = namedtuple('Flow_Dependency_i', 'From_action To_action')
Action_i = namedtuple('Action_i', 'ID')

pdb = "playdb"

class DivTest:
    """
    Test division example
    """
    @classmethod
    def play(cls):

        Database.open_session(name=pdb)
        Relvar.create_relvar(db=pdb, name='Action', attrs=[Attribute('ID', 'string')], ids={1: ['ID']})
        Relvar.create_relvar(db=pdb, name='Flow_Dependency', attrs=[
            Attribute('From_action', 'string'),
            Attribute('To_action', 'string'),
        ], ids={1: ['From_action', 'To_action']})

        Relvar.insert(db=pdb, relvar='Action', tuples=[
            Action_i(ID="ACTN1"),
            Action_i(ID="ACTN2"),
            Action_i(ID="ACTN3"),
            Action_i(ID="ACTN4"),
            Action_i(ID="ACTN5"),
            Action_i(ID="ACTN6"),
            Action_i(ID="ACTN7"),
            Action_i(ID="ACTN8"),
            Action_i(ID="ACTN9"),
            Action_i(ID="ACTN10"),
            Action_i(ID="ACTN11"),
            Action_i(ID="ACTN12"),
            Action_i(ID="ACTN13"),
        ])

        Relvar.insert(db=pdb, relvar='Flow_Dependency', tuples=[
            Flow_Dependency_i(From_action="ACTN1", To_action="ACTN4"),
            Flow_Dependency_i(From_action="ACTN1", To_action="ACTN2"),
            Flow_Dependency_i(From_action="ACTN2", To_action="ACTN3"),
            Flow_Dependency_i(From_action="ACTN3", To_action="ACTN7"),
            Flow_Dependency_i(From_action="ACTN4", To_action="ACTN5"),
            Flow_Dependency_i(From_action="ACTN5", To_action="ACTN6"),
            Flow_Dependency_i(From_action="ACTN6", To_action="ACTN7"),
            Flow_Dependency_i(From_action="ACTN7", To_action="ACTN9"),
            Flow_Dependency_i(From_action="ACTN8", To_action="ACTN9"),
            Flow_Dependency_i(From_action="ACTN9", To_action="ACTN13"),
            Flow_Dependency_i(From_action="ACTN9", To_action="ACTN12"),
            Flow_Dependency_i(From_action="ACTN10", To_action="ACTN11"),
            Flow_Dependency_i(From_action="ACTN11", To_action="ACTN13"),
            Flow_Dependency_i(From_action="ACTN11", To_action="ACTN12"),
            ])

        Relvar.printall(pdb)

        result = Relation.create(db=pdb, attrs=[Attribute(name="ID", type="string")],
                                 tuples=[
                                     Action_i(ID="ACTN1"),
                                     Action_i(ID="ACTN10"),
                                     Action_i(ID="ACTN8"),
                                 ])

        pass

