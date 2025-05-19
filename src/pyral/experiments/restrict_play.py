"""
restrict_play.py -- Test advanced restriction

"""
# System
from collections import namedtuple

# PyRAL
from pyral.database import Database
from pyral.relvar import Relvar
from pyral.relation import Relation
from pyral.rtypes import Attribute

Shaft_i = namedtuple('Shaft_i', 'ID Speed In_service')

ev = "ev"

def play():
    Database.open_session(ev)
    Relation.create(db=ev, attrs=[Attribute(name="ID", type="string"), Attribute(name="Speed", type="int"), Attribute(name="In_service", type="boolean")],
                    tuples=[
                        Shaft_i(ID='S1', Speed=31, In_service=True),
                        Shaft_i(ID='S2', Speed=14, In_service=False),
                        Shaft_i(ID='S3', Speed=20, In_service=True),
                    ], svar_name="shafts_rv")
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

    # R = f"ID:<{v}> OR (In_service:<{True}> AND Speed > <{s}>)"
    # R = f"NOT In_service:<{True}>"
    v = "S2"
    s = 14
    R = f"Speed > {s}, ID:<S3>"
    result = Relation.restrict6(db=ev, relation="shafts_rv", restriction=R, svar_name="restriction")
    Relation.print(db=ev, variable_name="restriction")

    pass

    # Relvar.create_relvar(db=ev, name='Shaft', attrs=[Attribute('ID', 'string'), Attribute('In_service', 'boolean'),],
    #                      ids={1: ['ID']})
    # Relvar.insert(db=ev, relvar='Shaft', tuples=[
    #     Shaft_i(ID='S1', In_service=True),
    #     Shaft_i(ID='S2', In_service=False),
    # ])

    # shaft_r = Relation.restrict(db=ev, relation="shafts_rv")




    Database.close_session(ev)

    pass
