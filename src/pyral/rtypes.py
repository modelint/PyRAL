"""
rtypes.py -- Relational database types such as header, attribute, tuple, etc
"""
# System
from typing import List
from collections import namedtuple
from enum import Enum

Attribute = namedtuple('_Attribute', 'name type')
RelationValue = namedtuple('RelationValue', 'name header body')
delim = '_' # TclRAL delimiter that replaces a space delimiter

# Commands
JoinCmd = namedtuple('JoinCmd', 'rname1 rname2 attrs')
SetCompareCmd = namedtuple('SetCompareCmd', 'rname1 rname2 op')
ProjectCmd = namedtuple('ProjectCmd', 'relation attributes')

class SetOp(Enum):
    subset = 'subsetof'
    superset = 'supersetof'
    psubset = 'propersubsetof'
    psuperset = 'propersupersetof'
    eq = 'equal'
    equal = 'equal'
    neq = 'notequal'
    notequal = 'notequal'

class Mult(Enum):
    AT_LEAST_ONE = '+'
    EXACTLY_ONE = '1'
    ZERO_ONE_OR_MANY = '*'
    ZERO_OR_ONE = '?'

def header(attrs: List[Attribute]) -> str:
    """
    Creates a header as a a bracketed list of attribute name pairs
    that can be spedified as part of a TclRAL command.

    Here's an example:
    {WPT_number int, Lat string, Long string, Frequency double}

    :param attrs: A tuple of attributes (name, type) pairs
    :return: A TclRAL header string
    """
    header_text = "{"
    for a in attrs:
        # We need to replace any spaces in an attribute name with underscores
        header_text += f"{a.name.replace(' ', delim)} {a.type.replace(' ', delim)} "
    return header_text[:-1] + "}"  # Replace the trailing space with a closing bracket

def body(tuples: List[namedtuple]) -> str:
    """
    A body is a set of tuples formated into a single line TclRAL string.

    Here's an example:

    {WPT_number 3 Lat {37° 46' 30" N} Long {-122° 25' 10"} Frequency 117.95}

    :return: A single line of text with a string of TclRAL tuples
    """
    # Add in all of the tuples
    body_text = ""
    for t in tuples:
        body_text += '{'
        instance_tuple = t._asdict()
        for k, v in instance_tuple.items():
            body_text += f"{k} {{{v}}} "  # Spaces are allowed in values
        body_text = body_text[:-1] + '} '
    return body_text[:-1]
