"""
create_play.py -- Test relational variable management

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

    Relvar.create_relvar(db=acdb, name='Fixed Wing Aircraft', attrs=[Attribute('ID', 'string'), Attribute('Altitude', 'int'),
                                                                     Attribute('Compass heading', 'int')], ids={1: ['ID']})

    attr_vals = [
        {'ID': 'N1397Q', 'Altitude': 13275, 'Compass_heading': 320},
        {'ID': 'N1309Z', 'Altitude': 10100, 'Compass_heading': 273},
        {'ID': 'N5130B', 'Altitude': 8159, 'Compass_heading': 90},
    ]

    result = Relvar.insert(db=acdb, relvar='Fixed Wing Aircraft', tuples=attr_vals)

    # Create a relation from a tuple
    # attrs = [Attribute(name='ID', type='string'), Attribute(name='Altitude', type='double'),
    #          Attribute(name='Compass_heading', type='int')]
    # values = [('N1397Q', 13275, 320)]


    # result = Relation.create(db=acdb, attrs=attrs, tuples=values)

    pass
