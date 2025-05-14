# test database management

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


#
# @pytest.fixture(scope='function', autouse=True)
# def tear_down_function():
#     yield
#     Relation.free_rvs(db="ac", owner="P1")

@pytest.fixture(scope='module')
def aircraft_db():
    acdb = "ac"

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
    return acdb

def test_rv_declare1(aircraft_db):
    join_example = Relation.declare_rv(db=aircraft_db, owner="P1", name="xjoin")
    Relation.join(db=aircraft_db, rname1="Pilot", rname2="Fixed Wing Aircraft", attrs={"Tail number": "ID"},
                  svar_name=join_example)
    assert Database.rv_names == {'ac': {'P1': {'xjoin'}}}
    Relation.free_rvs(db=aircraft_db, owner="P1")
    assert Database.rv_names == {aircraft_db: {}}

def test_rv_declare2(aircraft_db):
    join_example = Relation.declare_rv(db=aircraft_db, owner="P1", name="xjoin")
    Relation.join(db=aircraft_db, rname1="Pilot", rname2="Fixed Wing Aircraft", attrs={"Tail number": "ID"},
                  svar_name=join_example)
    semijoin_example = Relation.declare_rv(db=aircraft_db, owner="P1", name="xsemijoin")
    Relation.semijoin(db=aircraft_db, rname1="Pilot", rname2="Fixed Wing Aircraft", attrs={"Tail_number": "ID"},
                      svar_name=semijoin_example)
    assert Database.rv_names == {'ac': {'P1': {'xjoin', 'xsemijoin'}}}
    Relation.free_rvs(db=aircraft_db, owner="P1")
    assert Database.rv_names == {aircraft_db: {}}

def test_rv_free_bad_owner(aircraft_db):
    with pytest.raises(KeyError):
        Relation.free_rvs(db=aircraft_db, owner="P2")

def test_rv_bad_session(aircraft_db):
    with pytest.raises(KeyError):
        Relation.declare_rv(db="oink", owner="P1", name="xjoin")

def test_rv_duplicate_name(aircraft_db):
    Relation.declare_rv(db=aircraft_db, owner="P1", name="xjoin")
    with pytest.raises(KeyError):
        Relation.declare_rv(db=aircraft_db, owner="P1", name="xjoin")
