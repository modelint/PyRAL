""" test_dogs.py -- Test commands using the TclRAL manpage dog examples """

import pytest
from pyral.relation import Relation
from pyral.database import Database
from pyral.relvar import Relvar
from pyral.rtypes import Attribute, RelationValue

from collections import namedtuple

DOG_i = namedtuple('DOG_i', 'DogName')
OWNER_i = namedtuple('OWNER_i', 'OwnerName')
OWNERSHIP_i = namedtuple('OWNERSHIP_i', 'OwnerName DogName')
db= "dog_db"

@pytest.fixture(scope='module')
def open_db_session():
    Database.open_session(db)
    yield
    # don't close here — it's handled in tear_down

@pytest.fixture(autouse=True)
def tear_down(open_db_session):
    yield
    Database.close_session(db)

@pytest.fixture(scope='module')
def dog_db(open_db_session):
    Relvar.create_relvar(db=db, name="DOG", attrs=[
        Attribute(name="DogName", type="string")
    ], ids={1: ["DogName"]})
    Relvar.create_relvar(db=db, name="OWNER", attrs=[
        Attribute(name="OwnerName", type="string"),
        # Attribute(name="Age", type="int"),
        # Attribute(name="City", type="string"),
    ], ids={1: ["OwnerName"]})
    Relvar.create_relvar(db=db, name="OWNERSHIP", attrs=[
        Attribute(name="DogName", type="string"),
        Attribute(name="OwnerName", type="string")
    ], ids={1: ["OwnerName", "DogName"]})
    Relvar.insert(db=db, relvar='DOG', tuples=[
        DOG_i(DogName="Fido"),
        DOG_i(DogName="Sam"),
        DOG_i(DogName="Spot"),
        DOG_i(DogName="Rover"),
        DOG_i(DogName="Fred"),
        DOG_i(DogName="Jumper"),
    ])
    Relvar.insert(db=db, relvar='OWNER', tuples=[
        OWNER_i(OwnerName="Sue"),
        OWNER_i(OwnerName="George"),
        OWNER_i(OwnerName="Alice"),
        OWNER_i(OwnerName="Mike"),
        OWNER_i(OwnerName="Jim"),
    ])
    Relvar.insert(db=db, relvar='OWNERSHIP', tuples=[
        OWNERSHIP_i(OwnerName="Sue", DogName="Fido"),
        OWNERSHIP_i(OwnerName="Sue", DogName="Sam"),
        OWNERSHIP_i(OwnerName="George", DogName="Fido"),
        # OWNERSHIP_i(OwnerName="George", DogName="Sam"),
        OWNERSHIP_i(OwnerName="Alice", DogName="Spot"),
        OWNERSHIP_i(OwnerName="Mike", DogName="Rover"),
        OWNERSHIP_i(OwnerName="Jim", DogName="Fred"),
    ])


def test_divide(dog_db):
    # Dividend
    Relation.project(db=db, relation="DOG", attributes=("DogName",), svar_name="dividend")

    # Divisor
    R = f"OwnerName:<{'George'}> OR OwnerName:<{'Sue'}>"
    Relation.restrict(db=db, relation='OWNER', restriction=R)
    Relation.project(db=db, attributes=("OwnerName",), svar_name="divisor")

    # Mediator
    Relation.project(db=db, relation="OWNERSHIP", attributes=("OwnerName", "DogName"), svar_name="mediator")
    result = Relation.divide(db=db, dividend="dividend", divisor="divisor", mediator="mediator", svar_name="quotient")

    expected = RelationValue(name='^relation', header={'DogName': 'string'}, body=[{'DogName': 'Fido'}])
    assert result == expected

