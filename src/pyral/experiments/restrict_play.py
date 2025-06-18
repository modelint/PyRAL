"""
restrict_play.py -- Test advanced restriction

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
    n = "NOT here"
    R = f"Name:<{n}>"
    result = Relation.restrict(db=ev, relation="Shaft", restriction=R)
    pass
    # Relvar.printall(db=ev)
    # Relation.create(db=ev, attrs=[
    #     Attribute(name="ID", type="string"),
    #     Attribute(name="Speed", type="double"),
    #     Attribute(name="In_service", type="boolean")],
    #                 tuples=[
    #                     Shaft_i(ID='S1', Speed=31.3, In_service=False),
    #                     Shaft_i(ID='S2', Speed=14.2, In_service=False),
    #                     Shaft_i(ID='S3', Speed=20.16, In_service=False),
    #                 ], svar_name="shafts_rv")
    #
    # Relvar.set(db=ev, relvar="Shaft", relation="shafts_rv")
    # Relation.set()
    # Relation.raw(db=ev, cmd_str="relation restrictwith ${shafts_rv} {[expr {$Speed > 14}]}",
    #              svar_name="raw_rv")
    # Relation.raw(db=ev, cmd_str="relation restrictwith ${shafts_rv} {[string match S* {S2}]}",
    #              svar_name="raw_rv")
    # Relation.raw(db=ev, cmd_str="relation restrict ${shafts_rv} t {[string match {S2} [tuple extract $t ID]]}",
    #              svar_name="raw_rv")
    # Relation.print(db=ev, variable_name="raw_rv")
    pass

    # Relation.raw(db=ev, cmd_str=r"relation restrictwith ${shafts_rv} [ [string match {S1} $ID] && [string match {True} $In_service] ]",
    #              svar_name="raw_rv")
    # R = f"In_service:<{True}>"
    # R = f"ID:<{v}>, In_service:<{True}>"
    # R = f"ID:<{v}> OR In_service:<{True}>"

    v = "S1"
    s = 14.2
    # R = f"Speed > {s}, ID:<S3>"
    # R = f"Class:{v}"
    # R = f"ID==<S1>"
    # R = f"ID:<{v}>, In_service:<True>"
    # R = f"ID:<{v}>, Speed:<{s}>"
    # R = f"In_service:<{True}>"
    # Not working: R = f"ID:<{v}>, Speed:{s}"
    # R = f"Speed:{s}"
    R = f"Speed >= {s}"
    # R = f"NOT In_service:<{True}>"
    # R = f"NOT Speed:<{s}>"
    # s = 31
    # R = f"ID:<{v}> OR (In_service:<{True}> AND Speed > {s})"

    # result = Relation.restrict(db=ev, relation="shafts_rv", svar_name="restriction")
    # result = Relation.restrict(db=ev, relation="shafts_rv", restriction=R, svar_name="restriction")
    # Relation.print(db=ev, variable_name="restriction")

    # result = Relation.rank(db=ev, relation="shafts_rv", sort_attr_name="Speed", order=Order.DESCENDING)
    # result = Relation.rank_restrict(db=ev, relation="shafts_rv", attr_name="Speed", extent=Extent.GREATEST,
    #                                 card=Card.ONE, svar_name="rr_result")
    # Relation.tag(db=ev, relation="Shaft", tag_attr_name="_instance", svar_name="shaft_tagged")
    # Relation.project(db=ev, relation="shaft_tagged", attributes=("ID", "_instance"), svar_name="shaft_ids")
    # Relation.print(db=ev, variable_name="shaft_ids")
    Relvar.printall(db=ev)
    # Relation.print(db=ev, variable_name="Shaft")

    # pass

    # Relvar.create_relvar(db=ev, name='Shaft', attrs=[Attribute('ID', 'string'), Attribute('In_service', 'boolean'),],
    #                      ids={1: ['ID']})
    # Relvar.insert(db=ev, relvar='Shaft', tuples=[
    #     Shaft_i(ID='S1', In_service=True),
    #     Shaft_i(ID='S2', In_service=False),
    # ])

    # shaft_r = Relation.restrict(db=ev, relation="shafts_rv")

    Database.close_session(ev)

    pass
