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

Fixed_Wing_Aircraft_i = namedtuple('Fixed_Wing_Aircraft_i', 'ID Altitude Compass_heading')
Pilot_i = namedtuple('Pilot_i', 'Callsign Tail_number Age')


acdb = "ac"  # Flow database example
def play():
    Database.open_session(acdb)
    Relvar.create_relvar(db=acdb, name='Fixed Wing Aircraft', attrs=[Attribute('ID', 'string'), Attribute('Altitude', 'int'),
                                                       Attribute('Compass heading', 'int')], ids={1: ['ID']})
    Relvar.insert(db=acdb, relvar='Fixed Wing Aircraft', tuples=[
        Fixed_Wing_Aircraft_i(ID='N1397Q', Altitude=13275, Compass_heading=320),
        Fixed_Wing_Aircraft_i(ID='N1309Z', Altitude=10100, Compass_heading=273),
        Fixed_Wing_Aircraft_i(ID='N5130B', Altitude=8159, Compass_heading=90),
    ])

    Relvar.create_relvar(db=acdb, name='Pilot', attrs=[Attribute('Callsign', 'string'), Attribute('Tail_number', 'string'),
                                                    Attribute('Age', 'int')], ids={1: ['Callsign']})
    Relvar.insert(db=acdb, relvar='Pilot', tuples=[
        Pilot_i(Callsign='Viper', Tail_number='N1397Q', Age=22),
        Pilot_i(Callsign='Joker', Tail_number='N5130B', Age=31),
    ])


    result = Relation.join(db=acdb, rname1="Pilot", rname2="Fixed Wing Aircraft", attrs={"Tail number": "ID"}, svar_name="join")
    result = Relation.semijoin(db=acdb, rname1="Pilot", rname2="Fixed Wing Aircraft", attrs={"Tail_number": "ID"}, svar_name="semijoin")

    Relation.print(db=acdb, variable_name="Pilot")
    Relation.print(db=acdb, variable_name="Fixed Wing Aircraft")
    Relation.print(db=acdb, variable_name="join")
    Relation.print(db=acdb, variable_name="semijoin")
    pass
