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
from pyral.rtypes import *

Flowdep_i = namedtuple('Flowdep_i', 'From_action To_action')
Actionf_i = namedtuple('Actionf_i', 'From_action')
Actionta_i = namedtuple('Actionta_i', 'To_action')
Action_i = namedtuple('Action_i', 'ID')
ActionState_i = namedtuple('ActionState_i', 'ID State')

fdb = "fdb"  # Flow database example


def play():
    """
    Initialize the example
    """
    Database.open_session(name=fdb)
    action_states = 'Action States'
    flow_deps = 'flow_deps'
    unenabled_actions = 'unenabled_actions'

    Relvar.create_relvar(db=fdb, name=action_states, attrs=[
        Attribute('ID', 'string'),
        Attribute('State', 'string'),
    ], ids={1: ['ID']})
    Relvar.insert(db=fdb, relvar=action_states, tuples=[
        ActionState_i(ID="ACTN2", State="C"),
        ActionState_i(ID="ACTN1", State="U"),
        ActionState_i(ID="ACTN3", State="E"),
    ])
    Relation.print(db=fdb, variable_name=action_states)

    Relation.create(db=fdb, attrs=[
        Attribute('From_action', 'string'),
        Attribute('To_action', 'string'),
    ], tuples=[
        Flowdep_i(From_action="ACTN2", To_action="ACTN1")
    ], svar_name=flow_deps)
    Relation.print(db=fdb, variable_name=flow_deps)

    R = f"State:U"
    Relation.restrict(db=fdb, relation=action_states, restriction=R, svar_name=unenabled_actions)
    Relation.print(db=fdb, variable_name=unenabled_actions)

    # Build expr
    Relation.join(db=fdb, rname1=unenabled_actions, rname2=flow_deps, attrs={'ID': 'To_action'})
    Relation.print(db=fdb, table_name="Join1: Get From")

    Relation.semijoin(db=fdb, rname2=action_states, attrs={'From_action': 'ID'})
    Relation.print(db=fdb, table_name="Join2: Get status")

    R = f"NOT State:C"
    Relation.restrict(db=fdb, restriction=R)
    Relation.print(db=fdb, table_name="Not completed")

    # sum_expr = Relation.build_expr(commands=[
    #     JoinCmd(rname1="s", rname2=flow_deps, attrs={'ID': 'To_action'}),
    #     SemiJoinCmd(rname1=None, rname2=action_states, attrs={'From_action': 'ID'}),
    # ])
    #
    # s = Relation.summarize(db=fdb, relation="required_inputs", per_attrs=("To_action",),
    #                summaries=(SumExpr(attr=Attribute(name="Can_execute", type="boolean"), expr=sum_expr),),
    #                svar_name="solution")
    #
    # Relation.print(db=fdb, variable_name="solution")
    pass