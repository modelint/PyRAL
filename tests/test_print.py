# print tests

import pytest
from pyral.relation import Relation
from pyral.database import Database
from pyral.relvar import Relvar
from pyral.rtypes import Attribute, RelationValue

from collections import namedtuple

Aircraft_i = namedtuple('Aircraft_i', 'ID Altitude Heading')
Pilot_i = namedtuple('Pilot_i', 'Callsign Tail_number Age')

@pytest.fixture(scope='module', autouse=True)
def tear_down():
    yield
    Database.close_session("ac")

@pytest.fixture(scope='module')
def aircraft_db():
    acdb = "ac"

    Database.open_session(acdb)
    Relvar.create_relvar(db=acdb, name='Aircraft', attrs=[Attribute('ID', 'string'), Attribute('Altitude', 'int'),
                                                       Attribute('Heading', 'int')], ids={1: ['ID']})
    Relvar.insert(db=acdb, relvar='Aircraft', tuples=[
        Aircraft_i(ID='N1397Q', Altitude=13275, Heading=320),
        Aircraft_i(ID='N1309Z', Altitude=10100, Heading=273),
        Aircraft_i(ID='N5130B', Altitude=8159, Heading=90),
    ])

    Relvar.create_relvar(db=acdb, name='Pilot', attrs=[Attribute('Callsign', 'string'), Attribute('Tail_number', 'string'),
                                                    Attribute('Age', 'int')], ids={1: ['Callsign']})
    Relvar.insert(db=acdb, relvar='Pilot', tuples=[
        Pilot_i(Callsign='Viper', Tail_number='N1397Q', Age=22),
        Pilot_i(Callsign='Joker', Tail_number='N5130B', Age=31),
    ])
    return acdb


def test_compare_equal(aircraft_db):
    result = Relation.compare(db=aircraft_db, op='==', rname1='Aircraft', rname2='Aircraft')
    expected = True
    assert result == expected


def test_compare_not_equal(aircraft_db):
    result = Relation.compare(db=aircraft_db, op='!=', rname1='Aircraft', rname2='Aircraft')
    expected = False
    assert result == expected


def test_intersect(aircraft_db):
    cmd_high = 'set high [relation restrict $Aircraft t {[tuple extract $t Altitude] > 9000}]'
    cmd_low = 'set low [relation restrict $Aircraft t {[tuple extract $t Altitude] < 13000}]'
    Database.execute(db=aircraft_db, cmd=cmd_high)
    Database.execute(db=aircraft_db, cmd=cmd_low)
    Relation.print(db=aircraft_db, variable_name='high')
    Relation.print(db=aircraft_db, variable_name='low')
    b = Relation.intersect(db=aircraft_db, rname2='high', rname1='low')
    expected = RelationValue(name='^relation',
                             header={'ID': 'string', 'Altitude': 'int', 'Heading': 'int'},
                             body=[{'ID': 'N1309Z', 'Altitude': '10100', 'Heading': '273'}])
    assert b == expected
    Relation.relformat(b)

def test_cardinality(aircraft_db):
    c = Relation.cardinality(db=aircraft_db, rname="Aircraft")
    assert c == 3

def test_union(aircraft_db):
    R = f"ID:<N1397Q>"
    Relation.restrict(db=aircraft_db, relation='Aircraft', restriction=R)
    Relation.project(db=aircraft_db, attributes=("ID",), svar_name="a")
    R = f"ID:<N1309Z>"
    Relation.restrict(db=aircraft_db, relation='Aircraft', restriction=R)
    Relation.project(db=aircraft_db, attributes=("ID",), svar_name="b")
    R = f"ID:<N5130B>"
    Relation.restrict(db=aircraft_db, relation='Aircraft', restriction=R)
    Relation.project(db=aircraft_db, attributes=("ID",), svar_name="c")
    u = Relation.union(db=aircraft_db, relations=("a", "b", "c"))

    expected = RelationValue(name='^relation',
                             header={'ID': 'string'},
                             body=[{'ID': 'N1397Q'}, {'ID': 'N1309Z'}, {'ID': 'N5130B'}])
    Relation.relformat(u)
    assert u == expected


def test_join(aircraft_db):
    result = Relation.join(db=aircraft_db, rname2='Aircraft', rname1='Pilot',
                           attrs={'Tail_number': 'ID'}, svar_name='Joined')
    expected = RelationValue(name='^relation',
                             header={'Callsign': 'string', 'Tail_number': 'string', 'Age': 'int', 'Altitude': 'int',
                                     'Heading': 'int'},
                             body=[{'Callsign': 'Viper', 'Tail_number': 'N1397Q', 'Age': '22', 'Altitude': '13275',
                                    'Heading': '320'},
                                   {'Callsign': 'Joker', 'Tail_number': 'N5130B', 'Age': '31', 'Altitude': '8159',
                                    'Heading': '90'}])
    Relation.relformat(result)
    assert result == expected

def test_semijoin(aircraft_db):
    result = Relation.semijoin(db=aircraft_db, rname1='Pilot', rname2='Aircraft', attrs={'Tail number': 'ID'})
    expected = RelationValue(name='^relation',
                             header={'ID': 'string', 'Altitude': 'int', 'Heading': 'int'},
                             body=[{'ID': 'N1397Q', 'Altitude': '13275', 'Heading': '320'},
                                   {'ID': 'N5130B', 'Altitude': '8159', 'Heading': '90'}])
    Relation.relformat(result)
    assert result == expected


def test_selectid_found(aircraft_db):
    result = Relvar.select_id(db=aircraft_db, relvar_name='Aircraft', tid={'ID': 'N1397Q'})
    expected = RelationValue(name=None, header={'ID': 'string', 'Altitude': 'int', 'Heading': 'int'},
                             body=[{'ID': 'N1397Q', 'Altitude': '13275', 'Heading': '320'}])
    Relation.relformat(result)
    assert result == expected


def test_selectid_none(aircraft_db):
    result = Relvar.select_id(db=aircraft_db, relvar_name='Aircraft', tid={'ID': 'X'})
    expected = RelationValue(name=None, header={'ID': 'string', 'Altitude': 'int', 'Heading': 'int'},
                             body={})
    assert result == expected


def test_restrict(aircraft_db):
    R = f"ID:<N1397Q>"
    result = Relation.restrict(db=aircraft_db, relation='Aircraft', restriction=R)
    expected = RelationValue(name='^relation', header={'ID': 'string', 'Altitude': 'int', 'Heading': 'int'},
                             body=[{'ID': 'N1397Q', 'Altitude': '13275', 'Heading': '320'}])
    assert result == expected
