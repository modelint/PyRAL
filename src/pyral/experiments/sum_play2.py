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
from pyral.rtypes import Attribute, SetOp, JoinCmd, SetCompareCmd, ProjectCmd


Flow_Dependency_i = namedtuple('Flow_Dependency_i', 'From_action To_action')
Actionf_i = namedtuple('Actionf_i', 'From_action')
Actionta_i = namedtuple('Actionta_i', 'To_action')
Action_i = namedtuple('Action_i', 'ID')

fdb = "fdb"  # Flow database example

class SumTest2:
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

        # Specify a set of initial from actions that have completed execution
        Relation.create(db=fdb, attrs=[Attribute(name="From_action", type="string")],
                        tuples=[
                            Actionf_i(From_action="ACTN1"),
                            Actionf_i(From_action="ACTN10"),
                            Actionf_i(From_action="ACTN8"),
                        ], svar_name="xactions")
        Relation.print(db=fdb, variable_name="xactions")

        # Find all downstream dependencies
        Relation.join(db=fdb, rname1="Flow_Dependency", rname2="xactions")
        Relation.project(db=fdb, attributes=("To_action",), svar_name="downstream")
        Relation.print(db=fdb, variable_name="downstream")

        # Find all required upstream inputs for the downstream actions
        Relation.join(db=fdb, rname1="Flow_Dependency", rname2="downstream", svar_name="required_inputs")
        Relation.print(db=fdb, variable_name="required_inputs")

        # Add a boolean attribute named Can_execute
        # For each downstream action (per To_action) join it back to required inputs, projecting on the From_action
        # This yields the set of upstream executed actions for that one downstream action
        # Then test to see if this set is a subset of the xactions
        # If so, the downstream action has all dependencies fulfilled and can now execute (true)
        # Otherwise, there are some required input actions that have not just executed
        Relation.raw(
            db=fdb, cmd_str=r"relation summarizeby $required_inputs To_action s Can_execute boolean {\
            [relation is [relation project [relation join $required_inputs $s] From_action] subsetof $xactions]}",
            svar_name="c"
        )
        Relation.print(db=fdb, variable_name="c")

        Relation.create(db=fdb, attrs=[Attribute(name="From_action", type="string")],
                        tuples=[
                            Actionf_i(From_action="ACTN9"),
                        ], svar_name="s")
        Relation.print(db=fdb, variable_name="s")

        sum_expr = Relation.build_expr(commands=[
            JoinCmd(rname1="s", rname2="required_inputs", attrs=None),
            ProjectCmd(attributes=("From_action",), relation=None),
            SetCompareCmd(rname2="xactions", op=SetOp.subset, rname1=None)
        ])

        Relation.sumby(db=fdb, relation="required_inputs", per_attr="To_action",
                       ext_attr=Attribute(name="Can_execute", type="boolean"), sum_expr=sum_expr,
                       svar_name="solution")

        Relation.print(db=fdb, variable_name="solution")
        pass

        # Relation.join(db=fdb, rname2="required_inputs", rname1="s")
        # Relation.project(db=fdb, attributes=("From_action",))
        # Relation.set_compare(db=fdb, rname2="xactions", op=SetOp.subset)

        # relation join ${s} $required_inputs'
        # relation project ${^relation} From_action'
        # relation is ${^relation} subsetof $xactions'
