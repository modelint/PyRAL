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
        Relvar.create_relvar(db, name='Aircraft', attrs=[Attribute('ID', 'string'), Attribute('Altitude', 'int'),
                                                         Attribute('Heading', 'int')], ids={1: ['ID']})
        db.eval('relvar insert Aircraft {ID N1397Q Altitude 13275 Heading 320}')
        db.eval('relvar insert Aircraft {ID N1309Z Altitude 10100 Heading 273}')
        db.eval('relvar insert Aircraft {ID N5130B Altitude 8159 Heading 90}')
        Relation.print(db, "Aircraft")



