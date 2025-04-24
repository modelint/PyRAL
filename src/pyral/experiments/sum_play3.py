"""
sum_play3.py -- Play around with summarization

"""
# System
from collections import namedtuple

# PyRAL
from pyral.database import Database
from pyral.relvar import Relvar
from pyral.relation import Relation
from pyral.rtypes import Attribute, SetOp, JoinCmd, SetCompareCmd, ProjectCmd, SumExpr, Cardinality

Region_i = namedtuple('Region_i', 'Data_box Title_block_pattern Stack_order')

tdb = "tdb"  # Title block database example


class SumTest3:
    """
    Summarization example
    """

    @classmethod
    def setup(cls):
        """
        Initialize the example
        """
        Database.open_session(name=tdb)
        Relvar.create_relvar(db=tdb, name='Region', attrs=[
            Attribute('Data_box', 'int'),
            Attribute('Title_block_pattern', 'string'),
            Attribute('Stack_order', 'int'),
        ], ids={1: ['Data_box', 'Title_block_pattern', 'Stack_order']})

        Relvar.insert(db=tdb, relvar='Region', tuples=[
            Region_i(Data_box=3, Title_block_pattern="Complex", Stack_order=1),
            Region_i(Data_box=3, Title_block_pattern="Complex", Stack_order=2),
            Region_i(Data_box=3, Title_block_pattern="SE Simple", Stack_order=1),
            Region_i(Data_box=5, Title_block_pattern="SE Simple", Stack_order=1),
            Region_i(Data_box=6, Title_block_pattern="SE Simple", Stack_order=1),
            Region_i(Data_box=6, Title_block_pattern="SE Simple", Stack_order=2),
            Region_i(Data_box=7, Title_block_pattern="SE Simple", Stack_order=1),
            Region_i(Data_box=7, Title_block_pattern="SE Simple", Stack_order=2),
        ])

        Relvar.printall(tdb)

        cls.play()

    @classmethod
    def play(cls):

        Relation.sumby(db=tdb, relation="Region", per_attrs=("Data_box", "Title_block_pattern"),
                       summaries=(SumExpr(attr=Attribute(name="Qty", type="int"), expr=Cardinality),),
                       svar_name="solution")

        Relation.print(db=tdb, variable_name="solution")
        pass