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

# Fixed_Wing_Aircraft_i = namedtuple('Fixed_Wing_Aircraft_i', 'ID Altitude Compass_heading')
Pilot_i = namedtuple('Pilot_i', 'Callsign Tail_number Age')

acdb = "ac"  # Flow database example


def play():

    Database.open_session(acdb)

    Relvar.create_relvar(db=acdb, name='Pilot', attrs=[Attribute('Callsign', 'string'), Attribute('Tail_number', 'string'),
                                                       Attribute('Age', 'int')], ids={1: ['Callsign']})
    Relvar.insert(db=acdb, relvar='Pilot', tuples=[
        Pilot_i(Callsign='Viper', Tail_number='N1397Q', Age=22),
        Pilot_i(Callsign='Joker', Tail_number='N5130B', Age=31),
    ])

    Relvar.create_relvar(db=acdb, name='Fixed Wing Aircraft', attrs=[Attribute('ID', 'string'), Attribute('Altitude', 'int'),
                                                                     Attribute('Compass heading', 'int')], ids={1: ['ID']})

    attr_vals = [
        {'ID': 'N1397Q', 'Altitude': 13275, 'Compass heading': 320},
        {'ID': 'N1309Z', 'Altitude': 10100, 'Compass heading': 273},
        {'ID': 'N5130B', 'Altitude': 8159, 'Compass heading': 90},
    ]

    result_relvar = Relvar.insert(db=acdb, relvar='Fixed Wing Aircraft', tuples=attr_vals)

    # Create a relation from a tuple
    attrs = [Attribute(name='ID', type='string'), Attribute(name='Altitude msl', type='double'),
             Attribute(name='Compass_heading', type='int')]
    values = [('N1397Q', 13275, 320)]


    result_relation = Relation.create(db=acdb, attrs=attrs, tuples=values)

    pass
