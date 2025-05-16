"""
rv_play.py -- Test relational variable management

"""
# System
from collections import namedtuple
from typing import NamedTuple, Any

# PyRAL
from pyral.database import Database
from pyral.relvar import Relvar
from pyral.relation import Relation
from pyral.rtypes import Attribute

def declare_rvs(db: str, owner: str, *names: str) -> Any:
    """
    Declare multiple relation variables and return them as a dynamically named NamedTuple.
    The field names will be the declared names with '_rv' appended.
    """
    fields = [f"{name}" for name in names]
    values = [Relation.declare_rv(db=db, owner=owner, name=name) for name in names]

    RVDynamic = namedtuple("RVDynamic", fields)
    return RVDynamic(*values)


# See comment in scalar_switch.py
class RVs(NamedTuple):
    join_example: str
    semijoin_example: str

# This wrapper calls the imported declare_rvs function to generate a NamedTuple instance with each of our
# variables above as a member.
def declare_my_module_rvs(db: str, owner: str) -> RVs:
    rvs = declare_rvs(db, owner, "join_example", "semijoin_example")
    return RVs(*rvs)

Fixed_Wing_Aircraft_i = namedtuple('Fixed_Wing_Aircraft_i', 'ID Altitude Compass_heading')
Pilot_i = namedtuple('Pilot_i', 'Callsign Tail_number Age')


acdb = "ac"  # Flow database example
def play():

    Database.open_session(acdb)


    # Get a NamedTuple with a field for each relation variable name
    rv = declare_my_module_rvs(db=acdb, owner="P1")

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

    # join_example = Relation.declare_rv(db=acdb, owner="P1", name="join")
    # semi_join_example = Relation.declare_rv(db=acdb, owner="P1", name="semijoin")

    result1 = Relation.join(db=acdb, rname1="Pilot", rname2="Fixed Wing Aircraft", attrs={"Tail number": "ID"},
                            svar_name=rv.join_example)
    result2 = Relation.semijoin(db=acdb, rname1="Pilot", rname2="Fixed Wing Aircraft", attrs={"Tail_number": "ID"},
                                svar_name=rv.semijoin_example)

    Relation.print(db=acdb, variable_name=rv.join_example)
    Relation.print(db=acdb, variable_name=rv.semijoin_example)

    print(Database.rv_names)
    print("---")
    Relation.free_rvs(db=acdb, owner="P2")

    # Relation.free_rvs(db=acdb, owner="P1", names=("join_example",), exclude=True)
    print(Database.rv_names)
    Database.close_session(acdb)

    pass
