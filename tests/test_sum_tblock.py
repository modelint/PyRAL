""" test_sum_tblock.py -- Test summarization """

# System
import pytest
from collections import namedtuple

# PyRAL
from pyral.database import Database
from pyral.relvar import Relvar
from pyral.relation import Relation
from pyral.rtypes import Attribute, SumExpr, Cardinality, RelationValue

Region_i = namedtuple('Region_i', 'Data_box Title_block_pattern Stack_order')

db = "tdb"  # Title block database example


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
def titleblock_db(open_db_session):
    Relvar.create_relvar(db=db, name='Region', attrs=[
        Attribute('Data_box', 'int'),
        Attribute('Title_block_pattern', 'string'),
        Attribute('Stack_order', 'int'),
    ], ids={1: ['Data_box', 'Title_block_pattern', 'Stack_order']})

    Relvar.insert(db=db, relvar='Region', tuples=[
        Region_i(Data_box=3, Title_block_pattern="Complex", Stack_order=1),
        Region_i(Data_box=3, Title_block_pattern="Complex", Stack_order=2),
        Region_i(Data_box=3, Title_block_pattern="SE Simple", Stack_order=1),
        Region_i(Data_box=5, Title_block_pattern="SE Simple", Stack_order=1),
        Region_i(Data_box=6, Title_block_pattern="SE Simple", Stack_order=1),
        Region_i(Data_box=6, Title_block_pattern="SE Simple", Stack_order=2),
        Region_i(Data_box=7, Title_block_pattern="SE Simple", Stack_order=1),
        Region_i(Data_box=7, Title_block_pattern="SE Simple", Stack_order=2),
    ])


def test_sum_card(titleblock_db):
    result = Relation.summarize(db=db, relation="Region", per_attrs=("Data_box", "Title_block_pattern"),
                                summaries=(SumExpr(attr=Attribute(name="Qty", type="int"), expr=Cardinality),),
                                svar_name="solution")

    expected = RelationValue(name='^relation',
                             header={'Data_box': 'int', 'Title_block_pattern': 'string', 'Qty': 'int'},
                             body=[
                                 {'Data_box': '3', 'Title_block_pattern': 'Complex', 'Qty': '2'},
                                 {'Data_box': '3', 'Title_block_pattern': 'SE Simple', 'Qty': '1'},
                                 {'Data_box': '5', 'Title_block_pattern': 'SE Simple', 'Qty': '1'},
                                 {'Data_box': '6', 'Title_block_pattern': 'SE Simple', 'Qty': '2'},
                                 {'Data_box': '7', 'Title_block_pattern': 'SE Simple', 'Qty': '2'}
                             ])
    assert result == expected
