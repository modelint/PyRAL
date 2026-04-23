"""
rv_unset_play.py -- Test relational variable deletion

"""
# System
from collections import namedtuple
from typing import NamedTuple, Any

# PyRAL
from pyral.database import Database
from pyral.relvar import Relvar
from pyral.relation import Relation
from pyral.rtypes import Attribute

Fixed_Wing_Aircraft_i = namedtuple('Fixed_Wing_Aircraft_i', 'ID Altitude Compass_heading')

acdb = "ac"  # Flow database example
def play():

    Database.open_session(acdb)
    db_open = Database.get_open_sessions()

    # Get a NamedTuple with a field for each relation variable name
    my_aircraft_rv = Relation.declare_rv(db=acdb, owner='airport', name='my_aircraft')

    Relvar.create_relvar(db=acdb, name=my_aircraft_rv, attrs=[Attribute('ID', 'string'), Attribute('Altitude', 'int'),
                                                       Attribute('Compass heading', 'int')], ids={1: ['ID']})
    Relvar.insert(db=acdb, relvar=my_aircraft_rv, tuples=[
        Fixed_Wing_Aircraft_i(ID='N1397Q', Altitude=13275, Compass_heading=320),
        Fixed_Wing_Aircraft_i(ID='N1309Z', Altitude=10100, Compass_heading=273),
        Fixed_Wing_Aircraft_i(ID='N5130B', Altitude=8159, Compass_heading=90),
    ])

    before = Database.get_rv_names(db=acdb, p=True)
    print("---")
    Relation.free_rvs(db=acdb, owner="airport")
    after = Database.get_rv_names(db=acdb, p=True)

    Relvar.unset(db=acdb, relvar=my_aircraft_rv)
    # Relation.raw(
    #     db=acdb, cmd_str=f'relvar unset {my_aircraft_rv}'
    # )

    # Try to create it again
    my_aircraft_rv = Relation.declare_rv(db=acdb, owner='airport', name='my_aircraft')
    Relvar.create_relvar(db=acdb, name=my_aircraft_rv, attrs=[Attribute('ID', 'string'), Attribute('Altitude', 'int'),
                                                              Attribute('Compass heading', 'int')], ids={1: ['ID']})

    # Relation.free_rvs(db=acdb, owner="P1", names=("join_example",), exclude=True)
    Database.close_session(acdb)
    # afterclose = Database.get_rv_names(db=acdb)
    # afterdb = Database.get_open_sessions()

    pass
