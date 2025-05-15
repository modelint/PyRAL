"""
bool_play.py -- Test bool data type in Python and TclRAL

"""
# System
from collections import namedtuple

# PyRAL
from pyral.database import Database
from pyral.relvar import Relvar
from pyral.relation import Relation
from pyral.rtypes import Attribute

Shaft_i = namedtuple('Shaft_i', 'ID In_service')

ev = "ev"

def play():
    Database.open_session(ev)
    Relation.create(db=ev, attrs=[Attribute(name="ID", type="string"), Attribute(name="In_service", type="boolean")],
                    tuples=[
                        Shaft_i(ID='S1', In_service=True),
                        Shaft_i(ID='S2', In_service=True),
                    ], svar_name="shafts_rv")
    Relation.raw(db=ev, cmd_str="relation body $shafts_rv")
    pass

    # Relvar.create_relvar(db=ev, name='Shaft', attrs=[Attribute('ID', 'string'), Attribute('In_service', 'boolean'),],
    #                      ids={1: ['ID']})
    # Relvar.insert(db=ev, relvar='Shaft', tuples=[
    #     Shaft_i(ID='S1', In_service=True),
    #     Shaft_i(ID='S2', In_service=False),
    # ])

    shaft_r = Relation.restrict(db=ev, relation="shafts_rv")



    Relation.print(db=ev, variable_name="shafts_rv")

    Database.close_session(ev)

    pass
