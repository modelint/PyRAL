"""
print_table.py -- Test table printing

"""
from pyral.database import Database
from pyral.relvar import Relvar
from pyral.relation import Relation
from pyral.transaction import Transaction
from pyral.rtypes import Attribute

from collections import namedtuple

Aircraft_i = namedtuple('Aircraft_i', 'ID Altitude Heading')
Pilot_i = namedtuple('Pilot_i', 'Callsign Tailnumber Age')
Region_i = namedtuple('Region_i', 'Data_box Title_block_pattern Stack_order')
DataBox_i = namedtuple('DataBox_i', 'ID Pattern Name')

class SumTest:

    """
    Test summarizeby command for count / cardinality
    """
    @classmethod
    def do_r(cls):
        stdb = "stdb"

        Database.open_session(name=stdb)
        Relvar.create_relvar(db=stdb, name='Region', attrs=[
            Attribute('Data_box', 'int'),
            Attribute('Title_block_pattern', 'string'),
            Attribute('Stack_order', 'int')],ids={1: ['Data_box', 'Title_block_pattern', 'Stack_order']})

        Relvar.insert(db=stdb, relvar='Region', tuples=[
            Region_i(Data_box=3, Title_block_pattern='Complex', Stack_order=1),
            Region_i(Data_box=3, Title_block_pattern='Complex', Stack_order=2),
            Region_i(Data_box=3, Title_block_pattern='SE Simple', Stack_order=1),
            Region_i(Data_box=5, Title_block_pattern='SE Simple', Stack_order=1),
            Region_i(Data_box=6, Title_block_pattern='SE Simple', Stack_order=1),
            Region_i(Data_box=6, Title_block_pattern='SE Simple', Stack_order=2),
            Region_i(Data_box=7, Title_block_pattern='SE Simple', Stack_order=1),
            Region_i(Data_box=7, Title_block_pattern='SE Simple', Stack_order=2),
        ])
        Relvar.printall(stdb)

        result = Relation.summarizeby(db=stdb, relation='Region', attrs=['Data_box', 'Title_block_pattern'],
                                      sum_attr=Attribute(name='Qty', type='int'), svar_name="Number_of_regions")
        Relation.print(db=stdb, variable_name="Number_of_regions")
        pass

class TableTest:

    @classmethod
    def do_r(cls):
        acdb = "acdb"

        Database.open_session(name=acdb)
        Relvar.create_relvar(acdb, name='Aircraft', attrs=[Attribute('ID', 'string'), Attribute('Altitude', 'int'),
                                                           Attribute('Heading', 'int')], ids={1: ['ID']})
        Relvar.create_relvar(acdb, name='Pilot',
                             attrs=[Attribute('Callsign', 'string'), Attribute('Tailnumber', 'string'),
                                    Attribute('Age', 'int')], ids={1: ['Callsign']})
        tr_p = Transaction.open(acdb, "Pilot")
        Relvar.insert(acdb, tr=tr_p, relvar='Pilot', tuples=[
            Pilot_i(Callsign='Viper', Tailnumber='N1397Q', Age=22),
            Pilot_i(Callsign='Joker', Tailnumber='N5130B', Age=31),
        ])
        tr_a = Transaction.open(acdb, "Aircraft")
        Relvar.insert(acdb, tr=tr_a, relvar='Aircraft', tuples=[
            Aircraft_i(ID='N1397Q', Altitude=13275, Heading=320),
            Aircraft_i(ID='N1309Z', Altitude=10100, Heading=273),
            Aircraft_i(ID='N5130B', Altitude=8159, Heading=90),
        ])

        Transaction.execute(acdb, tr_p)
        Relation.print(acdb, "Pilot")
        Transaction.execute(acdb, tr_a)
        Relation.print(acdb, "Aircraft")
        result = Relation.restrict(acdb, relation='Aircraft')
        pass

        # aone = Relvar.select_id(acdb, 'Aircraft', {'ID': '1397Q'}, svar_name='One')
        # Relation.relformat(aone)

        # result = Relation.join(acdb, rname1='Pilot', rname2='Aircraft')
        #
        # # result = Relation.project(db, attributes=('Age',), relation='Pilot')
        # Relation.relformat(result)
        #
        # a = Relation.restrict(db, restriction=f"Altitude:<10100>", relation="Aircraft")
        # b = Relation.restrict(db, restriction=f"Altitude:<10100>", relation="Aircraft")
        # Relation.relformat(a)

        # db.eval('set high [relation restrict $Aircraft t {[tuple extract $t Altitude] > 9000}]' )
        # Relation.print(db, 'high')
        # db.eval('set low [relation restrict $Aircraft t {[tuple extract $t Altitude] < 13000}]' )
        # Relation.print(db, 'low')
        # #
        # b = Relation.intersect(db, rname2='high', rname1='low')
        # Relation.relformat(b)
        #
        # thesame = db.eval('relation is $Aircraft != $Aircraft')
        # print(thesame)
        #
        # thesame = Relation.compare(db, op='<=', rname1='Aircraft', rname2='Aircraft')
        # print(thesame)

        # db.eval('set between [relation intersect $high $low]' )
        # Relation.print(db, 'between')

        # lower = Relation.subtract(db, rname2='r', rname1='Aircraft')
        # Relation.relformat(lower)
