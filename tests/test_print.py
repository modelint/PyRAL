# print tests

import pytest
from pyral.relation import Relation
from pyral.database import Database
from pyral.relvar import Relvar
from pyral.transaction import Transaction
from pyral.rtypes import Attribute

from collections import namedtuple

# Xdata_tuple = namedtuple('Xdata_tuple', 'S')

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
