"""
print_table.py -- Test table printing

"""
from pyral.database import Database
from pyral.relvar import Relvar
from pyral.relation import Relation
from pyral.transaction import Transaction
from pyral.rtypes import Attribute


class TableTest:

    @classmethod
    def print_table(cls):
        db = Database.init()
        Relvar.create_relvar(db, name='X', attrs=[Attribute('S', 'string')], ids={1: ['S']})
        db.eval('relvar insert X {S {hello there}}')
        db.eval('relvar insert X {S {Stop requested == true}}')
        db.eval('relvar insert X {S {world}}')
        Relation.print(db, "X")

    @classmethod
    def do_r(cls):
        db = Database.init()
        Relvar.create_relvar(db, name='Aircraft',
                             attrs=[Attribute('Tailnumber', 'string'), Attribute('Altitude', 'int'),
                                    Attribute('Heading', 'int')], ids={1: ['Tailnumber']})
        db.eval('relvar insert Aircraft {Tailnumber N1397Q Altitude 13275 Heading 320}')
        db.eval('relvar insert Aircraft {Tailnumber N1309Z Altitude 10100 Heading 273}')
        db.eval('relvar insert Aircraft {Tailnumber N5130B Altitude 8159 Heading 90}')
        Relvar.create_relvar(db, name='Pilot', attrs=[Attribute('Callsign', 'string'), Attribute('Tailnumber', 'string'),
                                                      Attribute('Age', 'int')], ids={1: ['Callsign']})
        db.eval('relvar insert Pilot {Callsign Viper Tailnumber N1397Q Age 22}')
        db.eval('relvar insert Pilot {Callsign Joker Tailnumber N5130B Age 31}')
        Relation.print(db, "Aircraft")
        Relation.print(db, "Pilot")

        # result = Relation.join(db, rname1='Pilot', rname2='Aircraft')
        #
        # # result = Relation.project(db, attributes=('Age',), relation='Pilot')
        # Relation.relformat(result)
        #
        # a = Relation.restrict(db, restriction=f"Altitude:<10100>", relation="Aircraft")
        # b = Relation.restrict(db, restriction=f"Altitude:<10100>", relation="Aircraft")
        # Relation.relformat(a)

        db.eval('set high [relation restrict $Aircraft t {[tuple extract $t Altitude] > 9000}]' )
        Relation.print(db, 'high')
        db.eval('set low [relation restrict $Aircraft t {[tuple extract $t Altitude] < 13000}]' )
        Relation.print(db, 'low')

        b = Relation.intersect(db, rname2='high', rname1='low')
        Relation.relformat(b)

        # thesame = db.eval('relation is $Aircraft != $Aircraft')
        # print(thesame)

        thesame = Relation.compare(db, op='<=', rname1='Aircraft', rname2='Aircraft')
        print(thesame)



        # db.eval('set between [relation intersect $high $low]' )
        # Relation.print(db, 'between')

        # lower = Relation.subtract(db, rname2='r', rname1='Aircraft')
        # Relation.relformat(lower)
