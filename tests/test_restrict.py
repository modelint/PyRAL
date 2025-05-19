import pytest
from pyral.relation import Relation
from pyral.database import Database
from pyral.relvar import Relvar
from pyral.rtypes import Attribute, RelationValue
from collections import namedtuple

Fixed_Wing_Aircraft_i = namedtuple('Fixed_Wing_Aircraft_i', 'ID Altitude Compass_heading')
Pilot_i = namedtuple('Pilot_i', 'Callsign Tail_number Age')

@pytest.fixture(scope='module', autouse=True)
def tear_down():
    yield
    Database.close_session("ac")

@pytest.fixture(scope='module')
def aircraft_db():
    acdb = "ac"
    Database.open_session(acdb)

    Relvar.create_relvar(
        db=acdb,
        name='Fixed Wing Aircraft',
        attrs=[
            Attribute('ID', 'string'),
            Attribute('Altitude', 'int'),
            Attribute('Compass heading', 'int')
        ],
        ids={1: ['ID']}
    )
    Relvar.insert(db=acdb, relvar='Fixed Wing Aircraft', tuples=[
        Fixed_Wing_Aircraft_i(ID='N1397Q', Altitude=13275, Compass_heading=320),
        Fixed_Wing_Aircraft_i(ID='N1309Z', Altitude=10100, Compass_heading=273),
        Fixed_Wing_Aircraft_i(ID='N5130B', Altitude=8159, Compass_heading=90),
    ])

    Relvar.create_relvar(
        db=acdb,
        name='Pilot',
        attrs=[
            Attribute('Callsign', 'string'),
            Attribute('Tail_number', 'string'),
            Attribute('Age', 'int')
        ],
        ids={1: ['Callsign']}
    )
    Relvar.insert(db=acdb, relvar='Pilot', tuples=[
        Pilot_i(Callsign='Viper', Tail_number='N1397Q', Age=22),
        Pilot_i(Callsign='Joker', Tail_number='N5130B', Age=31),
    ])

    return acdb

def test_string_match_angle_brackets(aircraft_db):
    result = Relation.restrict(db=aircraft_db, relation='Pilot', restriction="Callsign:<Viper>")
    assert len(result.body) == 1
    assert result.body[0]['Callsign'] == 'Viper'

def test_string_match_colon_value(aircraft_db):
    result = Relation.restrict(db=aircraft_db, relation='Pilot', restriction="Callsign:Viper")
    assert len(result.body) == 1
    assert result.body[0]['Tail_number'] == 'N1397Q'

def test_numeric_comparison(aircraft_db):
    result = Relation.restrict(db=aircraft_db, relation='Fixed Wing Aircraft', restriction="Altitude > 10000")
    ids = {row['ID'] for row in result.body}
    assert ids == {'N1397Q', 'N1309Z'}

def test_logical_and(aircraft_db):
    result = Relation.restrict(
        db=aircraft_db,
        relation='Fixed Wing Aircraft',
        restriction="Altitude > 10000, Compass_heading:320"
    )
    ids = {row['ID'] for row in result.body}
    assert ids == {'N1397Q'}

def test_logical_or(aircraft_db):
    result = Relation.restrict(
        db=aircraft_db,
        relation='Fixed Wing Aircraft',
        restriction="ID:<N1309Z> OR ID:<N5130B>"
    )
    ids = {row['ID'] for row in result.body}
    assert ids == {'N1309Z', 'N5130B'}

def test_parentheses_and_or(aircraft_db):
    restriction = "(ID:<N1397Q>, Altitude > 13000) OR (ID:<N1309Z>, Altitude > 9000)"
    result = Relation.restrict(db=aircraft_db, relation='Fixed Wing Aircraft', restriction=restriction)
    ids = {row['ID'] for row in result.body}
    assert ids == {'N1397Q', 'N1309Z'}

def test_not_logic(aircraft_db):
    result = Relation.restrict(db=aircraft_db, relation='Pilot', restriction="NOT Age:<22>")
    callsigns = {row['Callsign'] for row in result.body}
    assert callsigns == {'Joker'}