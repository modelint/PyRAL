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
Actionfa_i = namedtuple('Actionfa_i', 'From_action')
Action_i = namedtuple('Action_i', 'ID')
DOG_i = namedtuple('DOG_i', 'DogName')
OWNER_i = namedtuple('OWNER_i', 'OwnerName')
OWNERSHIP_i = namedtuple('OWNERSHIP_i', 'OwnerName DogName')

pdb = "playdb"
ddb = "dogs"

class SumTest:
    """
    Summarization example
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

        Relation.create(db=pdb, attrs=[Attribute(name="From_action", type="string")],
                                 tuples=[
                                     Actionfa_i(From_action="ACTN1"),
                                     Actionfa_i(From_action="ACTN10"),
                                     Actionfa_i(From_action="ACTN8"),
                                 ], svar_name="upstream")

        Relation.print(db=pdb, variable_name="upstream")

        j = Relation.join(db=pdb, rname2="Flow_Dependency", rname1="upstream")
        ds = Relation.project(db=pdb, attributes=("To_action",), svar_name="downstream")

        Relation.print(db=pdb, variable_name="downstream")
        Relation.join(db=pdb, rname2="Flow_Dependency", rname1="downstream", svar_name="relevant")
        Relation.print(db=pdb, variable_name="relevant")
        Relation.rename(db=pdb, names={"From_action": "X"}, svar_name="mediator")
        Relation.rename(db=pdb, relation="upstream", names={"From_action": "X"}, svar_name="divisor")
        Relation.print(db=pdb, variable_name="divisor")
        Relation.print(db=pdb, variable_name="mediator")

        pass



        result = Relation.divide(db=pdb, dividend="downstream", divisor="divisor", mediator="mediator")


        pass

    @classmethod
    def dogs(cls):
        Database.open_session(name=ddb)
        Relvar.create_relvar(db=ddb, name="DOG", attrs=[
            Attribute(name="DogName", type="string")
        ], ids={1: ["DogName"]})
        Relvar.create_relvar(db=ddb, name="OWNER", attrs=[
            Attribute(name="OwnerName", type="string"),
            # Attribute(name="Age", type="int"),
            # Attribute(name="City", type="string"),
        ], ids={1: ["OwnerName"]})
        Relvar.create_relvar(db=ddb, name="OWNERSHIP", attrs=[
            Attribute(name="DogName", type="string"),
            Attribute(name="OwnerName", type="string")
        ], ids={1: ["OwnerName", "DogName"]})
        Relvar.insert(db=ddb, relvar='DOG', tuples=[
            DOG_i(DogName="Fido"),
            DOG_i(DogName="Sam"),
            DOG_i(DogName="Spot"),
            DOG_i(DogName="Rover"),
            DOG_i(DogName="Fred"),
            DOG_i(DogName="Jumper"),
        ])
        Relvar.insert(db=ddb, relvar='OWNER', tuples=[
            OWNER_i(OwnerName="Sue"),
            OWNER_i(OwnerName="George"),
            OWNER_i(OwnerName="Alice"),
            OWNER_i(OwnerName="Mike"),
            OWNER_i(OwnerName="Jim"),
        ])
        Relvar.insert(db=ddb, relvar='OWNERSHIP', tuples=[
            OWNERSHIP_i(OwnerName="Sue", DogName="Fido"),
            OWNERSHIP_i(OwnerName="Sue", DogName="Sam"),
            OWNERSHIP_i(OwnerName="George", DogName="Fido"),
            # OWNERSHIP_i(OwnerName="George", DogName="Sam"),
            OWNERSHIP_i(OwnerName="Alice", DogName="Spot"),
            OWNERSHIP_i(OwnerName="Mike", DogName="Rover"),
            OWNERSHIP_i(OwnerName="Jim", DogName="Fred"),
        ])
        Relation.project(db=ddb, relation="DOG", attributes=("DogName",), svar_name="dividend")
        Relation.print(db=ddb, variable_name="dividend")

        R = f"OwnerName:<{'George'}> OR OwnerName:<{'Sue'}>"
        Relation.restrict(db=ddb, relation='OWNER', restriction=R)
        Relation.project(db=ddb,attributes=("OwnerName", ), svar_name="divisor")
        Relation.print(db=ddb, variable_name="divisor")

        Relation.project(db=ddb, relation="OWNERSHIP", attributes=("OwnerName", "DogName"), svar_name="mediator")
        Relation.print(db=ddb, variable_name="mediator")

        Relation.divide(db=ddb, dividend="dividend", divisor="divisor", mediator="mediator", svar_name="quotient")
        Relation.print(db=ddb, variable_name="quotient")
