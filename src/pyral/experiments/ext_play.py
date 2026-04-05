"""
ext_play.py -- Play around with extend

"""
# System
from collections import namedtuple

# PyRAL
from pyral.database import Database
from pyral.relvar import Relvar
from pyral.relation import Relation
from pyral.transaction import Transaction
from pyral.rtypes import Attribute


Action_i = namedtuple('Action_i', 'ID')

fdb = "fdb"  # Flow database example

def play():
    """
    Initialize the example
    """
    Database.open_session(name=fdb)
    # Specify a set of initial from actions that have completed execution
    Relation.create(db=fdb, attrs=[Attribute(name="ID", type="string")],
                    tuples=[
                        Action_i(ID="ACTN1"),
                        Action_i(ID="ACTN10"),
                        Action_i(ID="ACTN8"),
                    ], svar_name="actions")

    Relation.print(db=fdb, variable_name="actions")

    # Relation.extend(db=fdb, relation="actions", attrs={'State': 'Hello there'}, svar_name="c")
    Relation.extend(db=fdb, relation="actions", attrs={'Current': 'C', 'Previous': 'U'}, svar_name="c")
    # r = Relation.extend(db=fdb, relation="actions", attrs={'State': 'U'}, svar_name="c")
    pass
    # Relation.extend(db=fdb, relation="actions", attrs={'Number': 1.2}, svar_name="c")
    # Relation.extend(db=fdb, relation="actions", attrs={'Number': 3}, svar_name="c")
    # Relation.extend(db=fdb, relation="actions", attrs={'Status': True}, svar_name="c")

    # Relation.raw(
    #     db=fdb, cmd_str=r'relation extend $actions e State string {"U"} Previous string {"X"}',
    #     svar_name="c"
    # )
    # Relation.raw(
    #     db=fdb, cmd_str=r'relation extend $actions e State string {{U}}',
    #     svar_name="c"
    # )
    Relation.print(db=fdb, variable_name="c")



