"""
emptyof_play.py -- Test the command

"""
# System
from collections import namedtuple

# PyRAL
from pyral.database import Database
from pyral.relvar import Relvar
from pyral.relation import Relation
from pyral.rtypes import Attribute, Order, Card, Extent

Shaft_i = namedtuple('Shaft_i', 'ID Speed Name In_service')

ev = "ev"


def play():
    Database.open_session(ev)
    Relvar.create_relvar(
        db=ev, name="Shaft", attrs=[
            Attribute(name="ID", type="string"),
            Attribute(name="Speed", type="double"),
            Attribute(name="Name", type="string"),
            Attribute(name="In_service", type="boolean")], ids={1: ["ID"]}
    )
    Relvar.insert(db=ev, relvar="Shaft", tuples=[
                        Shaft_i(ID='S1', Speed=31.3, Name="hello", In_service=True),
                        Shaft_i(ID='S2', Speed=14.2, Name="hi there", In_service=False),
                        Shaft_i(ID='S3', Speed=20.16, Name="NOT here", In_service=True),
                        Shaft_i(ID='S4', Speed=31.3, Name="A", In_service=True),
                    ]
    )

    # First do a normal restriction
    n = "NOT here"
    i = "S3"
    R = f"Name:<{n}>, ID:<{i}>"
    result = Relation.restrict(db=ev, relation="Shaft", restriction=R, svar_name="s_rv")
    Relation.print(db=ev, variable_name="s_rv")

    # Now get the empty body
    e_result = Relation.emptyof(db=ev, relation="Shaft", svar_name="e_rv")
    Relation.print(db=ev, variable_name="e_rv")

    # Select only a single tuple play
    Relation.tag(db=ev, relation="Shaft", svar_name="t_rv")
    Relation.print(db=ev, variable_name="t_rv")
    c = Relation.cardinality(db=ev, rname="t_rv")
    print(f"Selecting one with tag: {c}")
    import random
    stag = random.randrange(c)
    Relation.restrict(db=ev, relation="t_rv", restriction=f"_tag:<{stag}>", svar_name="c_rv")
    Relation.print(db=ev, variable_name="c_rv")
    pass
    Database.close_session(ev)
