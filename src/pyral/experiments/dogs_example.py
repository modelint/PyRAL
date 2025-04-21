"""
dogs_example.py -- Set up basic dogs example

"""
# System
from collections import namedtuple

# PyRAL
from pyral.database import Database
from pyral.relvar import Relvar
from pyral.relation import Relation
from pyral.transaction import Transaction
from pyral.rtypes import Attribute


DOG_i = namedtuple('DOG_i', 'DogName Breed')
OWNER_i = namedtuple('OWNER_i', 'OwnerName Age City')
OWNERSHIP_i = namedtuple('OWNERSHIP_i', 'OwnerName DogName Acquired')

ddb = "dogs"

class Dogs:
    @classmethod
    def setup(cls):
        Database.open_session(name=ddb)
        Relvar.create_relvar(db=ddb, name="DOG", attrs=[
            Attribute(name="DogName", type="string"),
            Attribute(name="Breed", type="string"),
        ], ids={1: ["DogName"]})
        Relvar.create_relvar(db=ddb, name="OWNER", attrs=[
            Attribute(name="OwnerName", type="string"),
            Attribute(name="Age", type="int"),
            Attribute(name="City", type="string"),
        ], ids={1: ["OwnerName"]})
        Relvar.create_relvar(db=ddb, name="OWNERSHIP", attrs=[
            Attribute(name="DogName", type="string"),
            Attribute(name="OwnerName", type="string"),
            Attribute(name="Acquired", type="string"),
        ], ids={1: ["OwnerName", "DogName"]})
        Relvar.insert(db=ddb, relvar='DOG', tuples=[
            DOG_i(DogName="Fido", Breed="Poodle"),
            DOG_i(DogName="Sam", Breed = "Collie"),
            DOG_i(DogName="Spot", Breed = "Terrier"),
            DOG_i(DogName="Rover", Breed = "Retriever"),
            DOG_i(DogName="Fred", Breed = "Spaniel"),
            DOG_i(DogName="Jumper", Breed = "Mutt"),
        ])
        Relvar.insert(db=ddb, relvar='OWNER', tuples=[
            OWNER_i(OwnerName="Sue", Age=24, City="Cupertino"),
            OWNER_i(OwnerName="George", Age=35, City="Sunnyvale"),
            OWNER_i(OwnerName="Alice", Age=30, City="Cupertino"),
            OWNER_i(OwnerName="Mike", Age=50, City="San Jose"),
            OWNER_i(OwnerName="Jim", Age=42, City="San Francisco"),
        ])
        Relvar.insert(db=ddb, relvar='OWNERSHIP', tuples=[
            OWNERSHIP_i(OwnerName="Sue", DogName="Fido", Acquired="2001"),
            OWNERSHIP_i(OwnerName="Sue", DogName="Sam", Acquired="2000"),
            OWNERSHIP_i(OwnerName="George", DogName="Fido", Acquired="2001"),
            OWNERSHIP_i(OwnerName="George", DogName="Sam", Acquired="2000"),
            OWNERSHIP_i(OwnerName="Alice", DogName="Spot", Acquired="2001"),
            OWNERSHIP_i(OwnerName="Mike", DogName="Rover", Acquired="2002"),
            OWNERSHIP_i(OwnerName="Jim", DogName="Fred", Acquired="2003"),
        ])

        Relvar.printall(db=ddb)
