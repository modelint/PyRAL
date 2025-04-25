""" test_sum_flow.py -- Test summarization """

# System
import pytest
from collections import namedtuple

# PyRAL
from pyral.database import Database
from pyral.relvar import Relvar
from pyral.relation import Relation
from pyral.rtypes import Attribute, SumExpr, RelationValue, SetOp, JoinCmd, SetCompareCmd, ProjectCmd

Flow_Dependency_i = namedtuple('Flow_Dependency_i', 'From_action To_action')
Actionf_i = namedtuple('Actionf_i', 'From_action')
Actionta_i = namedtuple('Actionta_i', 'To_action')
Action_i = namedtuple('Action_i', 'ID')

db = "fdb"  # Flow database example


@pytest.fixture(scope='module')
def open_db_session():
    Database.open_session(db)
    yield
    # don't close here — it's handled in tear_down


@pytest.fixture(autouse=True)
def tear_down(open_db_session):
    yield
    Database.close_session(db)


@pytest.fixture(scope='module')
def flow_db(open_db_session):
    Relvar.create_relvar(db=db, name='Flow_Dependency', attrs=[
        Attribute('From_action', 'string'),
        Attribute('To_action', 'string'),
    ], ids={1: ['From_action', 'To_action']})

    Relvar.insert(db=db, relvar='Flow_Dependency', tuples=[
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


def test_sum_expr(flow_db):
    # Specify a set of initial from actions that have completed execution
    Relation.create(db=db, attrs=[Attribute(name="From_action", type="string")],
                    tuples=[
                        Actionf_i(From_action="ACTN1"),
                        Actionf_i(From_action="ACTN10"),
                        Actionf_i(From_action="ACTN8"),
                    ], svar_name="xactions")
    Relation.print(db=db, variable_name="xactions")

    # Find all downstream dependencies
    Relation.join(db=db, rname1="Flow_Dependency", rname2="xactions")
    Relation.project(db=db, attributes=("To_action",), svar_name="downstream")
    Relation.print(db=db, variable_name="downstream")

    # Find all required upstream inputs for the downstream actions
    Relation.join(db=db, rname1="Flow_Dependency", rname2="downstream", svar_name="required_inputs")
    Relation.print(db=db, variable_name="required_inputs")

    # Add a boolean attribute named Can_execute
    # For each downstream action (per To_action) join it back to required inputs, projecting on the From_action
    # This yields the set of upstream executed actions for that one downstream action
    # Then test to see if this set is a subset of the xactions
    # If so, the downstream action has all dependencies fulfilled and can now execute (true)
    # Otherwise, there are some required input actions that have not just executed

    sum_expr = Relation.build_expr(commands=[
        JoinCmd(rname1="s", rname2="required_inputs", attrs=None),
        ProjectCmd(attributes=("From_action",), relation=None),
        SetCompareCmd(rname2="xactions", op=SetOp.subset, rname1=None)
    ])

    result = Relation.summarize(db=db, relation="required_inputs", per_attrs=("To_action",),
                                summaries=(SumExpr(attr=Attribute(name="Can_execute", type="boolean"), expr=sum_expr),),
                                svar_name="solution")

    expected = RelationValue(name='^relation', header={'To_action': 'string', 'Can_execute': 'boolean'},
                             body=[
                                 {'To_action': 'ACTN4', 'Can_execute': '1'},
                                 {'To_action': 'ACTN2', 'Can_execute': '1'},
                                 {'To_action': 'ACTN9', 'Can_execute': '0'},
                                 {'To_action': 'ACTN11', 'Can_execute': '1'}
                             ])
    assert result == expected
