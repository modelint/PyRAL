"""
semiminus_play.py -- Play around with semiminus

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
ActionState_i = namedtuple('ActionState_i', 'ID Status')

fdb = "fdb"  # Flow database example

def play():
    """
    Initialize the example
    """
    Database.open_session(name=fdb)
    # Specify a set of initial from actions that have completed execution
    Relation.create(db=fdb, attrs=[Attribute(name="ID", type="string"), Attribute(name="Status", type="string")],
                    tuples=[
                        ActionState_i(ID="ACTN1", Status="U"),
                        ActionState_i(ID="ACTN2", Status="U"),
                        ActionState_i(ID="ACTN3", Status="U"),
                    ], svar_name="action_states")

    Relation.print(db=fdb, variable_name="action_states")

    Relation.create(db=fdb, attrs=[Attribute(name="ID", type="string")],
                    tuples=[
                        Action_i(ID="ACTN1"),
                        Action_i(ID="ACTN2"),
                    ], svar_name="enable_actions")

    Relation.print(db=fdb, variable_name="enable_actions")

    Relation.semiminus(db=fdb, rname1="enable_actions", rname2="action_states", svar_name="unchanged")
    Relation.semijoin(db=fdb, rname1="action_states", rname2="enable_actions")
    Relation.extend(db=fdb, attrs={"Status": "E"}, svar_name="enable_states")
    Relation.union(db=fdb, relations=("enable_states", "unchanged"), svar_name="newstates")

    Relation.print(db=fdb, variable_name="unchanged")
    Relation.print(db=fdb, variable_name="newstates")

    # Relation.raw(
    #     db=fdb, cmd_str=r'relation semiminus $enable_actions $action_states',
    #     svar_name="unchanged"
    # )



