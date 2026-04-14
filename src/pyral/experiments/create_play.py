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

    # Create a relation from a tuple
    attrs = [Attribute(name='ID', type='string'), Attribute(name='Altitude', type='double'),
             Attribute(name='Compass_heading', type='int')]
    values = [('N1397Q', 13275, 320)]

    Database.open_session(acdb)
    db_open = Database.get_open_sessions()

    result = Relation.create(db=acdb, attrs=attrs, tuples=values)

    pass
