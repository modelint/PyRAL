"""
sum_play.py -- Play around with summarization

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
Actionta_i = namedtuple('Actionta_i', 'To_action')
Action_i = namedtuple('Action_i', 'ID')

fdb = "fdb"  # Flow database example

class SumTest:
    """
    Summarization example
    """

    @classmethod
    def setup(cls):
        """
        Initialize the example
        """
        Database.open_session(name=fdb)
        Relvar.create_relvar(db=fdb, name='Flow_Dependency', attrs=[
            Attribute('From_action', 'string'),
            Attribute('To_action', 'string'),
        ], ids={1: ['From_action', 'To_action']})

        Relvar.insert(db=fdb, relvar='Flow_Dependency', tuples=[
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

        Relvar.printall(fdb)

        cls.play()

    @classmethod
    def play(cls):

        Relation.create(db=fdb, attrs=[Attribute(name="ID", type="string")],
                        tuples=[
                            Action_i(ID="ACTN1"),
                            Action_i(ID="ACTN10"),
                            Action_i(ID="ACTN8"),
                        ], svar_name="xactions")
        Relation.print(db=fdb, variable_name="xactions")

        Relation.join(db=fdb, rname1="Flow_Dependency", rname2="xactions", attrs={"From_action":"ID"}, svar_name="iflows")
        Relation.print(db=fdb, variable_name="iflows")
        pass

        Relation.project(db=fdb, attributes=("To_action",), relation="iflows", svar_name="downstream")
        Relation.print(db=fdb, variable_name="downstream")

        R = f"To_action:<ACTN9>"
        Relation.restrict(db=fdb, relation='downstream', restriction=R, svar_name="ds_tup")
        Relation.print(db=fdb, variable_name="ds_tup")

        Relation.join(db=fdb, rname2="Flow_Dependency", rname1="ds_tup")
        Relation.project(db=fdb, attributes=("From_action",), svar_name="up1")
        Relation.print(db=fdb, variable_name="up1")
        Relation.raw(db=fdb, cmd_str="relation rename $Flow_Dependency From_action Hello", svar_name="test")
        Relation.print(db=fdb, variable_name="test")


