"""
union_play.py -- Play around with union

"""
# System
from collections import namedtuple

# PyRAL
from pyral.database import Database
from pyral.relvar import Relvar
from pyral.relation import Relation
from pyral.rtypes import Attribute

Aircraft_i = namedtuple('Aircraft_i', 'ID Altitude Heading')


aircraft_db = "ac"  # Flow database example
def play():
    Database.open_session(aircraft_db)
    Relvar.create_relvar(db=aircraft_db, name='Aircraft', attrs=[Attribute('ID', 'string'), Attribute('Altitude', 'int'),
                                                       Attribute('Heading', 'int')], ids={1: ['ID']})
    Relvar.insert(db=aircraft_db, relvar='Aircraft', tuples=[
        Aircraft_i(ID='N1397Q', Altitude=13275, Heading=320),
        Aircraft_i(ID='N1309Z', Altitude=10100, Heading=273),
        Aircraft_i(ID='N5130B', Altitude=8159, Heading=90),
    ])
    R = f"ID:<N1397Q>"
    Relation.restrict(db=aircraft_db, relation='Aircraft', restriction=R)
    Relation.project(db=aircraft_db, attributes=("ID",), svar_name="a")
    R = f"ID:<N1309Z>"
    Relation.restrict(db=aircraft_db, relation='Aircraft', restriction=R)
    Relation.project(db=aircraft_db, attributes=("ID",), svar_name="b")
    R = f"ID:<N5130B>"
    Relation.restrict(db=aircraft_db, relation='Aircraft', restriction=R)
    Relation.project(db=aircraft_db, attributes=("ID",), svar_name="c")
    Relation.union(db=aircraft_db, relations=("a", "b", "c"), svar_name="u")

    Relation.print(db=aircraft_db, variable_name="u")
    pass
