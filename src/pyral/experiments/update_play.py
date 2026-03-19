"""
update_play.py -- Play around with relvar updates

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
ActionState_i = namedtuple('ActionState_i', 'ID State')

fdb = "fdb"  # Flow database example

def play():
    """
    Initialize the example
    """
    Database.open_session(name=fdb)
    Relvar.create_relvar(db=fdb, name='_Action_States', attrs=[
        Attribute('ID', 'string'),
        Attribute('State', 'string'),
    ], ids={1: ['ID']})

    Relvar.insert(db=fdb, relvar='_Action_States', tuples=[
        ActionState_i(ID="ACTN1", State="U"),
        ActionState_i(ID="ACTN2", State="U"),
        ActionState_i(ID="ACTN3", State="U"),
    ])

    Relation.print(db=fdb, variable_name="_Action_States")

    # Relation.create(db=fdb, attrs=[Attribute(name="ID", type="string"), Attribute(name="State", type="string")],
    #                 tuples=[
    #                     ActionState_i(ID="ACTN1", State="E"),
    #                     ActionState_i(ID="ACTN2", State="E"),
    #                 ], svar_name="enable_actions")
    #
    # Relation.print(db=fdb, variable_name="enable_actions")

    Relation.raw(
        db=fdb, cmd_str=r'relvar updateone _Action_States t {ID ACTN1} {tuple update $t State E}'
    )
    Relation.raw(
        db=fdb, cmd_str=r'relvar updateone _Action_States t {ID ACTN2} {tuple update $t State E}'
    )
    # Relation.raw(
    #     db=fdb, cmd_str=r'relvar update _Action_States t {[tuple extract $t ID] eq "ACTN1"} {'
    #                     r'tuple update $t State "E"}]',
    # )
    # Relation.raw(
    #     db=fdb, cmd_str=r'relvar updateper _Action_States [relation table {ID string State string} {ACTN1 E} {ACTN2 E}]',
    # )
    # Relation.raw(
    #     db=fdb, cmd_str=r'relvar updateper _Action_States $enable_actions',
    # )
    Relation.print(db=fdb, variable_name="_Action_States")



