# print tests

import pytest
from pyral.relation import Relation
from pyral.database import Database
from pyral.relvar import Relvar
from pyral.transaction import Transaction
from pyral.rtypes import Attribute, RelationValue

from collections import namedtuple


# Xdata_tuple = namedtuple('Xdata_tuple', 'S')
@pytest.fixture
def aircraft_db():
    db = Database.init()
    Relvar.create_relvar(db, name='Aircraft', attrs=[Attribute('ID', 'string'), Attribute('Altitude', 'int'),
                                                     Attribute('Heading', 'int')], ids={1: ['ID']})
    db.eval('relvar insert Aircraft {ID N1397Q Altitude 13275 Heading 320}')
    db.eval('relvar insert Aircraft {ID N1309Z Altitude 10100 Heading 273}')
    db.eval('relvar insert Aircraft {ID N5130B Altitude 8159 Heading 90}')
    Relvar.create_relvar(db, name='Pilot', attrs=[Attribute('Callsign', 'string'), Attribute('Tailnumber', 'string'),
                                                  Attribute('Age', 'int')], ids={1: ['Callsign']})
    db.eval('relvar insert Pilot {Callsign Viper Tailnumber N1397Q Age 22}')
    db.eval('relvar insert Pilot {Callsign Joker Tailnumber N5130B Age 31}')
    return db


def test_compare_equal(aircraft_db):
    result = Relation.compare(aircraft_db, op='==', rname1='Aircraft', rname2='Aircraft')
    expected = True
    assert result == expected

def test_compare_not_equal(aircraft_db):
    result = Relation.compare(aircraft_db, op='!=', rname1='Aircraft', rname2='Aircraft')
    expected = False
    assert result == expected

def test_intersect(aircraft_db):
    aircraft_db.eval('set high [relation restrict $Aircraft t {[tuple extract $t Altitude] > 9000}]')
    Relation.print(aircraft_db, 'high')
    aircraft_db.eval('set low [relation restrict $Aircraft t {[tuple extract $t Altitude] < 13000}]')
    Relation.print(aircraft_db, 'low')
    b = Relation.intersect(aircraft_db, rname2='high', rname1='low')
    expected = RelationValue(name='^relation',
                             header={'ID': 'string', 'Altitude': 'int', 'Heading': 'int'},
                             body=[{'ID': 'N1309Z', 'Altitude': '10100', 'Heading': '273'}])
    assert b == expected
    Relation.relformat(b)


def test_join(aircraft_db):
    result = Relation.join(aircraft_db, rname2='Aircraft', rname1='Pilot',
                           attrs={'Tailnumber': 'ID'}, svar_name='Joined')
    expected = RelationValue(name='^relation',
             header={'Callsign': 'string', 'Tailnumber': 'string', 'Age': 'int', 'Altitude': 'int', 'Heading': 'int'},
             body=[{'Callsign': 'Viper', 'Tailnumber': 'N1397Q', 'Age': '22', 'Altitude': '13275', 'Heading': '320'},
                   {'Callsign': 'Joker', 'Tailnumber': 'N5130B', 'Age': '31', 'Altitude': '8159', 'Heading': '90'}])
    Relation.relformat(result)
    assert result == expected

def test_selectid_found(aircraft_db):
    result = Relvar.select_id(aircraft_db, relvar_name='Aircraft', tid={'ID': 'N1397Q'})
    expected = RelationValue(name=None, header={'ID': 'string', 'Altitude': 'int', 'Heading': 'int'},
                             body=[{'ID': 'N1397Q', 'Altitude': '13275', 'Heading': '320'}])
    Relation.relformat(result)
    assert result == expected

def test_selectid_none(aircraft_db):
    result = Relvar.select_id(aircraft_db, relvar_name='Aircraft', tid={'ID': 'X'})
    expected = RelationValue(name=None, header={'ID': 'string', 'Altitude': 'int', 'Heading': 'int'},
                             body={})
    assert result == expected

def test_restrict(aircraft_db):
    R = f"ID:<N1397Q>"
    result = Relation.restrict(aircraft_db, relation='Aircraft', restriction=R)
    expected = RelationValue(name='^relation', header={'ID': 'string', 'Altitude': 'int', 'Heading': 'int'},
                             body=[{'ID': 'N1397Q', 'Altitude': '13275', 'Heading': '320'}])
    assert result == expected


@pytest.fixture
def create_test_db():
    db = Database.init()
    Relvar.create_relvar(db, name='X', attrs=[Attribute('S', 'string')], ids={1: ['S']})
    return db


def test_simple_insert(create_test_db):
    create_test_db.eval('relvar insert X {S {hello there}}')
    result = create_test_db.eval('set X')
    assert result == '{S string} {{S {hello there}}}'


def test_symbol_insert(create_test_db):
    create_test_db.eval('relvar insert X {S {Stop requested == true}}')
    result = create_test_db.eval('set X')
    assert result == '{S string} {{S {Stop requested == true}}}'
