"""
relation.py – Operations on relations
"""

# System
import logging
import re
from tabulate import tabulate
from typing import List, Optional, Dict, Tuple
from collections import namedtuple

# PyRAL
from pyral.rtypes import RelationValue, Attribute, header, body, SetOp, SumExpr, snake, Order, Card, Extent
from pyral.database import Database

_logger = logging.getLogger(__name__)

# If we want to apply successive (nested) operations in TclRAL we need to have the result
# of each TclRAL command saved in tcl variable. So each time we execute a command that produces
# a relation result we save it. The variable name is chosen so that it shouldn't conflict with
# any user relvars. Do not ever use the name below as one of your user relvars!
# For any given command, if no relvar is specified, the previous relation result is assumed
# to be the input.
_relation = r'^relation'  # Name of the latest relation result. Carat prevents name collision
_RANK = "_rank"  # Default name of the rank attribute added by extension using the rank command
_TAG = "_tag" # Default name of the tag attribute added by the tag command
session_variable_names = set()  # Maintain a list of temporary variable names in use


class Relation:
    """
    A relational value
    """

    @classmethod
    def declare_rv(cls, db: str, owner: str, name: str) -> str:
        """
        Saves

        :param db:
        :param owner:
        :param name:
        :return:
        """
        # Verify that db session exists
        if db not in Database.sessions:
            raise KeyError(f"Database session '{db}' has not been initialized.")

        db_rvs = Database.rv_names.setdefault(db,{})
        owner_rvs = db_rvs.setdefault(owner, set())

        if name in owner_rvs:
            raise KeyError(f"Relational variable {name} already defined for owner {owner}")

        owner_rvs.add(name)
        return f"{owner}__{name}"

    @classmethod
    def free_rvs(cls, db: str, owner: str, names: tuple[str, ...] = (), exclude: bool = False):
        """
        Unset relation variable names declared by the owner.

        :param db: Database session name
        :param owner: Name of the owner who declared the relational variables
        :param names: Names to include or exclude (depending on `exclude`)
        :param exclude: If True, *keep* the listed names and delete all others.
                        If False, *delete only* the listed names. If names is empty, delete all.
        """
        try:
            owner_rvs = Database.rv_names[db][owner]
        except KeyError:
            raise KeyError(f"No such owner '{owner}' in session '{db}'")

        names_to_remove = (
            owner_rvs - set(names) if exclude and names
            else set(names) if names
            else owner_rvs
        )

        for name in names_to_remove:
            cmd = f"unset {owner}__{name}"
            Database.execute(db=db, cmd=cmd)

        # Remove updated set of RVs or delete owner entry entirely if now empty
        remaining = owner_rvs - names_to_remove
        if remaining:
            Database.rv_names[db][owner] = remaining
        else:
            Database.rv_names[db].pop(owner, None)
            if not Database.rv_names[db]:
                Database.rv_names.pop(db, None)

    @classmethod
    def summarize(cls, db: str, per_attrs: Tuple[str, ...], summaries: Tuple[SumExpr], relation: str = _relation,
                  svar_name: Optional[str] = None) -> RelationValue:
        """
        Full implementation of summarize/summarizeby

        :param db: DB session name
        :param per_attrs:
        :param summaries:
        :param relation:
        :param svar_name:
        :return:
        """
        sum_expr_strings = [f"{s.attr.name} {s.attr.type} {{[{s.expr}]}}" for s in summaries]
        summaries_clause = ' '.join(sum_expr_strings)
        # cmd = (f"set {_relation} [relation summarizeby ${{{relation}}} {{{' '.join(per_attrs)}}} s "
        #        f"{summaries_clause}"
        cmd = (f"set {_relation} [relation summarizeby ${{{relation}}} {{{' '.join(per_attrs)}}} s "
               f"{summaries_clause}]")
        result = Database.execute(db=db, cmd=cmd)
        if svar_name:  # Save the result using the supplied session variable name
            cls.set_var(db=db, name=svar_name)
        return cls.make_pyrel(result)

    @classmethod
    def _expand_expr(cls, cmd_strings: List[str]) -> str:
        """
        Collapse a list of TclRAL command strings into a single string with nested commands.

        For example, these three input cmd_strings:

            0 'relation is ${^relation} subsetof $xactions'
            1 'relation project ${^relation} From_action'
            2 'relation join ${s} $required_inputs'

        Collapse into this returned command string:

            'relation is [relation project [relation join ${s} $required_inputs] From_action] subsetof $xactions'

        Expression expansion is indicated by the _relation variable which contains the TclRAL variable name
        representing the output yield by the previously executed command. As of this writing, the variable name
        is: ${^relation}

        The first command in the list contains the text _relation value somewhere in the string

        We need to replace this value with the next command string surrounded in brackets [<command_string>].
        But, unless that command is the last one in the list, it too will contain the _relation text,
        requiring further expansion.

        So we descend recursively until we hit the bottom command. There we simply return the command text surrounded
        in brackets replacing as we go for each _relation occurence moving back up the stack.

        :param cmd_strings: A list of TclRAL command strings in reverse nesting order
        :return: The fully expanded TclRAL command with all _relation occcurences replaced
        """
        # Ensure that we have either started with at least one command or that we haven't
        # somehow recursed beyond the end of the original command list
        if len(cmd_strings) < 1:
            raise ValueError("At least one command must be specified for expression expansion.")

        # If we have reached the last command, there is no further recursion and we just return the command string
        if len(cmd_strings) == 1:  # Last one, cannot flatten any further, so just return it
            if _relation in cmd_strings[0]:
                raise ValueError(f"Final command: [{cmd_strings[0]}] must not contain substitution marker: {_relation}")
            return cmd_strings[0]

        # There should be exactly one appearance of the _relation
        if _relation in cmd_strings[0]:
            r_expr = cls._expand_expr(cmd_strings[1:])
            expansion = cmd_strings[0].replace(f"${{{_relation}}}", f"[{r_expr}]", 1)
            return expansion
        else:
            raise ValueError(f"Non-final command: [{cmd_strings[0]}] must contain substitution marker: {_relation}")


    @classmethod
    def build_expr(cls, commands) -> str:
        """
        Builds a nested TclRAL expression command for use with summarize

        For example, let's say we want to combine this list of PyRAL Relation commands
        into a single TclRAL expression:

            Relation.join(db=fdb, rname2="required_inputs", rname1="s")
            Relation.project(db=fdb, attributes=("From_action",))
            Relation.set_compare(db=fdb, rname2="xactions", op=SetOp.subset)

        We will receive these as a list of corresponding namedtuples:

            JoinCmd(rname1="s", rname2="required_inputs", attrs=None),
            ProjectCmd(attributes=("From_action",), relation=None),
            SetCompareCmd(rname2="xactions", op=SetOp.subset, rname1=None)


        :param commands:
        :return:
        """

        # Here is each named tuple defining a Relation method that can be built into an expression
        relation_method = {
            "SetCompareCmd": lambda c: cls._cmd_set_compare(rname1=c.rname1, rname2=c.rname2, op=c.op),
            "ProjectCmd": lambda c: cls._cmd_project(relation=c.relation, attributes=c.attributes),
            "JoinCmd": lambda c: cls._cmd_join(rname1=c.rname1, rname2=c.rname2, attrs=c.attrs),
        }

        # Now we create a list of command strings in reverse order
        # This order makes it easy to nest the commands into a single TclRAL command string
        cmd_strings = []
        for c in reversed(commands):
            tuple_name = type(c).__name__
            try:
                cmd = relation_method[tuple_name](c)
            except KeyError:
                raise ValueError(f"Unknown relation command namedtuple: {tuple_name}")
            cmd_strings.append(cmd)

        # Now call this recursive function to perform command substitution and yield our single TclRAL command string
        return cls._expand_expr(cmd_strings)  # TODO: need to specify the db

    @classmethod
    def _cmd_set_compare(cls, rname2: str, op: SetOp, rname1: Optional[str] = None) -> str:
        if rname1 is None:
            rname1 = _relation
        return f'relation is ${{{snake(rname1)}}} {op.value} ${snake(rname2)}'

    @classmethod
    def _cmd_project(cls, attributes, relation: Optional[str] = None) -> str:
        if relation is None:
            relation = _relation
        return f"relation project ${{{snake(relation)}}} {' '.join(attributes)}"

    @classmethod
    def _cmd_union(cls, relations) -> str:
        rvars = [f"${snake(r)}" for r in relations]
        return f'relation union {" ".join(rvars)}'

    @classmethod
    def _cmd_join(cls, rname2: str, attrs, rname1: Optional[str] = None) -> str:
        if rname1 is None:
            rname1 = _relation
        using = f" -using {cls.make_attr_list(attrs)}" if attrs else ""
        return f"relation join ${{{snake(rname1)}}} ${snake(rname2)}{using}"

    @classmethod
    def _cmd_semijoin(cls, rname2: str, attrs, rname1: Optional[str] = None) -> str:
        if rname1 is None:
            rname1 = _relation
        using = f" -using {cls.make_attr_list(attrs)}" if attrs else ""
        return f"relation semijoin ${{{snake(rname1)}}} ${snake(rname2)}{using}"

    @classmethod
    def set_compare(cls, db: str, rname2: str, op: SetOp, rname1: str = _relation) -> bool:
        """

        :param db: DB session name
        :param rname1: If not specified, the previous relation result
        :param rname2: Each rname must have the same header
        :param op: A SetOp enumeration element defined in rtypes.py
        :return: The boolean result of the set operation
        """
        cmd = cls._cmd_set_compare(rname1=snake(rname1), rname2=snake(rname2), op=op)
        result = bool(int(Database.execute(db=db, cmd=cmd)))
        return result

    @classmethod
    def create(cls, db: str, attrs: List[Attribute], tuples: List[namedtuple],
               svar_name: Optional[str] = None) -> RelationValue:
        """
        Create a relation

        :param db: DB session name
        :param attrs: A tuple of attributes (name, type) pairs
        :param tuples: A list of tuples named such that the attributes exactly match the relvar header
        :param svar_name: Relation result is stored in this optional TclRAL variable for subsequent operations to use
        :return: Resulting relation as a PyRAL relation value
        """
        h = header(attrs)
        b = body(tuples)
        cmd = f'set {_relation} [relation create {h} {b}]'
        result = Database.execute(db=db, cmd=cmd)
        if svar_name:  # Save the result using the supplied session variable name
            cls.set_var(db=db, name=svar_name)
        return cls.make_pyrel(result)

    @classmethod
    def build_select_expr(cls, selection: str) -> str:
        """
        Convert a Scrall style select expression to an equivalent Tcl string match expression

        For now we only support an and'ed list of direct string matches in the format:

           attr1:str1; attr2:str2, ...

        With the assumption that we would like to select each tuple where

        attr1 == str1 AND attr2 == str2 ...

        We'll convert this to a Tcl expression like this:

        {[string match str1 $attr1] && [string match str2 $attr2] ...}

        Note that this only works for the TclRAL relation restrictwith command and not the
        relation restrict command. But that should suffice for our purposes

        Once our Scrall parser is ready, we can expand the functionality further

        :param selection:  The Scrall style select expression
        :return: The Tcl expression
        """
        # Parse out matches on comma delimiter as a list of strings
        match_strings = selection.split(';')
        # Break each match on the ':' into attr and value as a dictionary
        attr_vals = {a[0].strip(): a[1].strip() for a in [m.split(':') for m in match_strings]}
        # Now build the selection expression from each dictionary item
        sexpr = "{"  # Selection expression is surrounded by brackets
        for attribute, value in attr_vals.items():
            # We AND them all together with the && tcl operator
            sexpr += f"[string match {{{value}}} ${attribute}] && "
        # Remove the trailing && and return the complete selection expression
        return sexpr.rstrip(' &&') + "}"

    @classmethod
    def set_var(cls, db: str, name: str):
        """
        Set a temporary TclRAL relation variable to the most recent returned result.
        This allows us to save a particular TclRAL return value string so that we can plug it
        into a subsequent TclRAL operation.

        :param db: DB session name
        :param name: The variable name (must be a legal Tcl variable name
        """
        session_variable_names.add(name)
        Database.sessions[db].eval(f"set {name} ${{{_relation}}}")

    @classmethod
    def make_attr_list(cls, attrs: Dict[str, str]) -> str:
        """
        Makes a TclRAL attrList to be inserted in a command
        :param attrs:
        :return:
        """
        attr_list = "{"
        for k, v in attrs.items():
            attr_list += f"{snake(k)} {v} "
        return attr_list[:-1] + "}"

    @classmethod
    def semijoin(cls, db: str, rname2: str, attrs: Optional[Dict[str, str]] = None, rname1: str = _relation,
             svar_name: Optional[str] = None) -> RelationValue:
        """
        Perform a semi join on two relations using an optional attribute mapping. If no attributes are specified,
        the semi-join is performed on same named attributes.

        :param db: DB session name
        :param rname1: Name of one relvar to join
        :param rname2: Name of the other relvar
        :param attrs: Dictionary in format { r1.attr_name: r2.attr_name, ... }

        :param db: DB session name
        :param svar_name: Relation result is stored in this optional TclRAL variable for subsequent operations to use
        :return Resulting relation as a TclRAL string


        ---
        Copied from the TclRAL man page:

        The semijoin subcommand computes the join of relationValue1 and relationValue2 but eliminates all of the
        attributes of relationValue1 (or alternatively speaking, projecting all attributes of relationValue2).

        The returned relation has a heading the same as relationValue2 and a body consisting of those tuples in
        relationValue2 that would have been included in the natural join with relationValue1.
        As with join, if the -using argument are missing, the join is computed across the attributes in
        relationValue1 and relationValue2 that are named the same. Otherwise the attrList argument is treated
        the same as for the join subcommand.

        Also like the join subcommand, additional relationValue arguments may be given and the result is computed
        in left to right order.

        This implies that the type of the result is always the type of the right most relationValue.
        N.B. the sense of this command is inverted from previous versions of this library.
        """
        if attrs is None:
            attrs = {}
        cmd = f"set {{{_relation}}} [{cls._cmd_semijoin(rname1=rname1, rname2=rname2, attrs=attrs)}]"
        result = Database.execute(db=db, cmd=cmd)
        if svar_name:  # Save the result using the supplied session variable name
            cls.set_var(db=db, name=svar_name)
        return cls.make_pyrel(result)

    @classmethod
    def join(cls, db: str, rname2: str, attrs: Optional[Dict[str, str]] = None, rname1: str = _relation,
             svar_name: Optional[str] = None) -> RelationValue:
        """
        Perform a natural join on two relations using an optional attribute mapping. If no attributes are specified,
        the join is performed on same named attributes.

        :param db: DB session name
        :param rname1: Name of one relvar to join
        :param rname2: Name of the other relvar
        :param attrs: Dictionary in format { r1.attr_name: r2.attr_name, ... }
        :param svar_name: Relation result is stored in this optional TclRAL variable for subsequent operations to use
        :return Resulting relation as a TclRAL string
        """
        if attrs is None:
            attrs = {}
        cmd = f"set {{{_relation}}} [{cls._cmd_join(rname1=rname1, rname2=rname2, attrs=attrs)}]"
        result = Database.execute(db=db, cmd=cmd)
        if svar_name:  # Save the result using the supplied session variable name
            cls.set_var(db=db, name=svar_name)
        return cls.make_pyrel(result)

    @classmethod
    def rename(cls, db: str, names: Dict[str, str], relation: str = _relation,
               svar_name: Optional[str] = None) -> RelationValue:
        """
        (NOTE: I only just NOW realized that the TclRAL join command provides an option to specify multiple renames
        as part of a join, because, of course it does! Argghh. So there may not be any need to perform multiple renames
        at once, but hey, there's no harm in providing potentially superfluous functionality as it does at least
        handle the single rename case. - LS)

        Given an input relation, rename one or more attributes from old to new names. This is useful when you want
        to join two relvars on attributes with differing names.

        In SM xUML, it is common for an attribute with one name to reference another attribute of the same
        type but a different name, Employee_ID -> Manager, for exmaple.

        We often need to rename multiple attributes before performing a join, so the single attribute rename
        operation provided by TclRAL is executed once for each element of the names dictionary.

        TclRAL rename syntax:
            relation rename <relationValue> ?oldname newname ...?

        Multiple rename example in TclRAL:
            relation rename ${Attribute_Reference} To_attribute Name
            relation rename ${^relation} To_class Class

        (^relation) is the name of PyRAL's intermediate result session variable, so we are feeding
        the result of the first rename into the next.

        Generated from the PyRAL input:
            relation: 'Attribute_Reference'
            names: {'To_attribute': 'Name', 'To_class': 'Class'}

        :param db: DB session name
        :param relation: The relation to rename
        :param names: Dictionary in format { old_name: new_name }
        :param svar_name:  Name of a TclRAL session variable named for future reference
        :return Resulting relation as a TclRAL string
        """
        r = relation
        result = None
        # Each attribute rename is executed with a separate command
        for old_name, new_name in names.items():
            # The first rename operation is on the supplied relation
            cmd = f'set {_relation} [relation rename ${{{r}}} {old_name} {new_name}]'
            result = Database.execute(db, cmd)
            r = _relation  # Subsequent renames are based on the previous result
        if svar_name:  # Save the final result using the supplied session variable name
            cls.set_var(db, svar_name)
        return cls.make_pyrel(result)  # Result of the final rename (all renames in place)

    @classmethod
    def intersect(cls, db: str, rname2: str, rname1: str = _relation, svar_name: Optional[str] = None
                 ) -> RelationValue:
        """
        Returns the intersection of two relations using the TclRAL intersect command.

        Each relation must be of the same type (same header) as will the result.

        The body of the result consists of those tuples present in both r1 and r2.

        Relational intersection is commutative so the order of the r1 and r2 arguments is not significant.

        The TclRAL syntax is:
            relation intersect <relationValue1> <relationValue2>

        :param db: DB session name
        :param rname1:
        :param rname2:
        :param svar_name: Relation result is stored in this optional TclRAL variable for subsequent operations to use
        :return Subtraction relation as a TclRAL string
        """
        cmd = f'set {_relation} [relation intersect ${{{rname1}}} ${rname2}]'
        result = Database.execute(db, cmd)
        if svar_name:  # Save the result using the supplied session variable name
            cls.set_var(db, svar_name)
        return cls.make_pyrel(result)

    @classmethod
    def compare(cls, db: str, op: str, rname2: str, rname1: str = _relation) -> bool:
        """
        Returns the intersection of two relations using the TclRAL intersect command.

        Each relation must be of the same type (same header) as will the result.

        The body of the result consists of those tuples present in both r1 and r2.

        Relational intersection is commutative so the order of the r1 and r2 arguments is not significant.

        The TclRAL syntax is:
            relation intersect <relationValue1> <relationValue2>

        :param db: DB session name
        :param op:  Comparison operation
        :param rname1:
        :param rname2:
        :param svar_name: Relation result is stored in this optional TclRAL variable for subsequent operations to use
        :return Subtraction relation as a TclRAL string
        """
        cmd = f'relation is ${{{snake(rname1)}}} {op} ${snake(rname2)}'
        result = bool(int(Database.execute(db=db, cmd=cmd)))
        return result


    @classmethod
    def cardinality(cls, db:str, rname: str = _relation) -> int:
        """
        The cardinality subcommand returns the number tuples contained in the body of the relation.

        :param db: DB session name
        :param rname: The tuples in the body of this relation are counted
        :return:
        """
        cmd = f'relation cardinality ${{{snake(rname)}}}'
        result = int(Database.execute(db=db, cmd=cmd))
        return result

    @classmethod
    def subtract(cls, db: str, rname2: str, rname1: str = _relation, svar_name: Optional[str] = None
                 ) -> RelationValue:
        """
        Returns the set difference between two relations using the TclRAL minus command.

        Each relation must be of the same type (same header) as will the result.

        The body of the result consists of those tuples present in r1 but not present in r2.

        Relational subtraction is not commutative so the order of the r1 and r2 arguments is significant.

        The TclRAL syntax is:
            relation minus <relationValue1> <relationValue2>

        TclRAL example taken from the lineage.py Derive method where a set of all classes playing one or more
        subclass roles subtracts all classes playing superclass roles to obtain a set of leaf classes that
        participate as subclasses only.

            relation minus $subs $supers

        Generated from the following PyRAL input:
            rname1: subs
            rname2: supers

        :param tclral: The TclRAL session
        :param rname1: Subtracts value in rname2
        :param rname2: Is subtracted from value in rname1
        :param svar_name: Relation result is stored in this optional TclRAL variable for subsequent operations to use
        :return Subtraction relation as a TclRAL string
        """
        cmd = f'set {_relation} [relation minus ${{{rname1}}} ${rname2}]'
        result = Database.execute(db, cmd)
        if svar_name:  # Save the result using the supplied session variable name
            cls.set_var(db, svar_name)
        return cls.make_pyrel(result)

    @classmethod
    def get_rval_string(cls, db: str, variable_name: str) -> str:
        """
        Obtain a relation from a TclRAL variable

        :param db: DB session name
        :param variable_name: Name of a variable containing a relation defined in that session
        :return: TclRAL string representing the relation value
        """
        return Database.execute(db, cmd=f"set {variable_name}")

    @classmethod
    def make_pyrel(cls, relation: str, name: str = _relation) -> RelationValue:
        """
        Take a relation obtained from TclRAL and convert it into a pythonic relation value.
        A RelationValue is a named tuple with a header and a body component.
        The header component will be a dictionary of attribute name keys and type values
        The body component will be a list of relational tuples each defined as a dictionary
        with a key matching some attribute of the header and a value for that attribute.

        :param relation: A TclRAL string representing a relation
        :param name: An optional relvar name
        :return: A RelationValue constructed from the provided relation string
        """
        # First check for the dee/dum edge cases
        if relation.strip() == '{} {}':
            # Tabledum (DUM), no attributes and no tuples (relational false value)
            # Header is an empty dictionary and body is an empty list
            return RelationValue(name=name, header={}, body=[])
        if relation.strip() == '{} {{}}':
            # Tabledum (DEE), no attributes and one empty tuple (relational true value)
            # Header is an empty dictionary and body is a list with one empty dictionary element
            return RelationValue(name=name, header={}, body=[{}])

        # Going forward we can assume that there is at least one attribute and zero or more tuples
        h, b = relation.split('}', 1)  # Split at the first closing bracket to obtain header and body strings

        # Construct the header dictionary
        h_items = h.strip('{').split()  # Remove the open brace and split on spaces (no spaces in TclRAL attr names)
        header = dict(zip(h_items[::2], h_items[1::2]))  # Attribute names for keys and TclRAL types for values

        # Construct the body list
        # Each tuple is surrounded by brackets so our first stop is to split them all out into distinct tuple strings
        # Remove leading space and enclosing brackets from entire body
        # 2/-2 skips over leading space and first bracket and truncates final bracket surrounding body
        body = b[2:-2].split('} {')
        body[0] = body[0].lstrip('{')  # Remove preceding bracket from the first tuple

        # Each tuple alternates with the attribute name and the attribute value
        # We want to extract just the values to create the table rows
        # To complicate matters, values may contain spaces. TclRAL attribute names do not.
        # A multi-word value is surrounded by brackets
        # So you might see a tuple like this: Floor_height 32.6 Name {Lower lobby}
        # We need a regex component that will extract the bracketed space delimited values
        # As well as the non-bracketed single word values
        # value_pattern = r"([{}<>\w ]*)"  # Grab a string of any combination of brackets, word characters and spaces
        value_pattern = r"(.*)"  # Grab the whole value string. We'll strip the brackets out later.
        # Now we build this component into an alternating pattern of attribute and value items
        # for the attributes in our relation header
        tuple_pattern = ""
        for a in header.keys():
            tuple_pattern += f"{a} {value_pattern} "
        tuple_pattern = tuple_pattern.rstrip(' ')  # Removes the final trailing space
        # Now we can use the constructed tuple pattern regex to extract a list of values
        # from each row to match our attribute list order
        # Here we apply the tuple_pattern regex to each body row stripping the brackets from each value
        # and end up with a list of unbracketed body row values

        # For tabulate we need a list for the columns and a list of lists for the rows

        # Handle case where there are zero body tuples
        at_least_one_tuple = b.strip('{} ')  # Empty string if no tuples in body
        if not at_least_one_tuple:
            return RelationValue(name=name, header=header, body={})

        # There is at least one body tuple
        if len(header) > 1:
            # More than one column and the regex match returns a convenient tuple in the zero element
            # b_rows = [for row in body]
            b_rows = [[f.strip('{}') for f in re.findall(tuple_pattern, row)[0]] for row in body]
        else:
            # If there is only one match (value), regex returns a string rather than a tuple
            # in the zero element. We need to embed this string in a list
            b_rows = [[re.findall(tuple_pattern, row)[0].strip('{}')] for row in body]
        # Either way, b_rows is a list of lists

        body = [dict(zip(header.keys(), r)) for r in b_rows]
        rval = RelationValue(name=name, header=header, body=body)
        return rval

    @classmethod
    def print(cls, db: str, variable_name: str = _relation, table_name: Optional[str] = None, printout: bool = True) -> str:
        """
        Given the name of a TclRAL relation variable, obtain its value and print it as a table.

        :param db: DB session name
        :param variable_name: Name of the TclRAL variable to print, also used to name the table if no table_name
        :param table_name:  If supplied, this name is used instead of the variable name to name the printed table
        :param printout: Print to console if true
        """
        # convert the TclRAL string value held in the session variable into a PyRAL relation and print it
        rval = cls.make_pyrel(relation=cls.get_rval_string(db=db, variable_name=snake(variable_name)),
                              name=table_name if table_name else variable_name)
        return cls.relformat(rval, printout)


    @classmethod
    def relformat(cls, rval: RelationValue, printout: bool = True) -> str:
        """
        Formats the PyRAL relation into a table and prints it using the imported tabulation module

        :param rval: A PyRAL relation value
        """
        # Now we have what we need to generate a table
        # Print the relvar name if supplied, otherwise use the default name for the latest result
        tablename = rval.name if rval.name else '<unnamed>'
        print(f"\n-- {tablename} --")
        attr_names = list(rval.header.keys())
        brows = [list(row.values()) for row in rval.body]
        table_text = tabulate(tabular_data=brows, headers=attr_names, tablefmt="outline")
        # That last parameter chooses our table style
        if printout:
            print(table_text)
        return table_text

    @classmethod
    def union(cls, db: str, relations: Tuple[str, ...], svar_name: Optional[str] = None) -> RelationValue:
        """
        The union subcommand returns the set union of two or more relations.
        All relations must be of the same type.

        The result relation has a heading that is the same as any of the arguments and
        has a body consist of all tuples present in any of the relationValue arguments.

        Since the union operation is both associative and commutative, the order of the
        relationValue arguments has no effect the result.

        :param db: DB session name
        :param relations: A tuple providing a
        :param svar_name: Relation result is stored in this optional TclRAL variable for subsequent operations to use
        :return Resulting relation as a PyRAL relation value
        """
        relations_s = (snake(t) for t in relations)
        cmd = f'set {_relation} [{cls._cmd_union(relations=relations_s)}]'
        result = Database.execute(db=db, cmd=cmd)
        if svar_name:  # Save the result using the supplied session variable name
            cls.set_var(db=db, name=svar_name)
        return cls.make_pyrel(result)

    @classmethod
    def divide(cls, db: str, dividend: str, divisor: str, mediator: str,
               svar_name: Optional[str] = None) -> RelationValue:
        """
        The divide subcommand implements the relational divide operation.

        The headings of dividend and divisor must be disjoint and the heading of mediator must be
        the union of the dividend and divisor headings.

        The returned result is a new relation that has the same heading as dividend and contains
        all the tuples from dividend whose corresponding tuples in mediator include all the tuples in divisor.
        Stated another way, the result of divide subcommand is the maximal set of tuples from dividend whose
        Cartesian product with divisor is completely contained in mediator.

        :param db: DB session name
        :param dividend: The dividend relation name
        :param divisor: The divisor relation name
        :param mediator: The mediator relation name
        :param svar_name: Relation result is stored in this optional TclRAL variable for subsequent operations to use
        :return Resulting relation as a PyRAL relation value
        """
        cmd = f'set {_relation} [relation divide ${dividend} ${divisor} ${mediator}]'
        result = Database.execute(db=db, cmd=cmd)
        if svar_name:  # Save the result using the supplied session variable name
            cls.set_var(db=db, name=svar_name)
        return cls.make_pyrel(result)

    @classmethod
    def raw(cls, db: str, cmd_str: str, relation: str = _relation,
                svar_name: Optional[str] = None) -> RelationValue:
        """
        Passes tcl cmd txt straight through, but uses the variable and relation
        naming mechanism to pipeline input and output like all other commands
        """
        cmd = f'set {_relation} [{cmd_str}]'
        result = Database.execute(db=db, cmd=cmd)
        if svar_name:  # Save the result using the supplied session variable name
            cls.set_var(db=db, name=svar_name)
        return cls.make_pyrel(result)

    @classmethod
    def heading(cls, db:str, relation: str = _relation):
        cmd = f"relation heading ${{{relation}}}"
        result = Database.execute(db=db, cmd=cmd)
        return result

    @classmethod
    def project(cls, db: str, attributes: Tuple[str, ...], exclude: bool = False, relation: str = _relation,
                svar_name: Optional[str] = None) -> RelationValue:
        """
        Returns a relation whose heading consists of only a set of selected attributes.
        The body of the result consists of the corresponding tuples from the specified relation,
        removing any duplicates created by considering only a subset of the attributes.

        :param db: DB session name
        :param attributes: Attributes to be projected
        :param relation: The relation to be projected
        :param exclude: If true, all attributes will be returned except for those in the attributes tuple
        :param svar_name: Relation result is stored in this optional TclRAL variable for subsequent operations to use
        :return Resulting relation as a PyRAL relation value
        """
        # Create a list of attributes to project by inclusion or exclusion
        if exclude:
            # Use heading method to get all attributes defined on the relation
            # and then project on all of these except those attributes provided in the tuple
            attr_types = Relation.heading(db=db, relation=relation)
            # heading returns a string delimited by spaces with attribute type pairs
            tokens = attr_types.split()
            # Now skip over all the type names and just grab the attribute names (1st, 3rd, ...)
            pairs = zip(tokens[::2], tokens[1::2])
            # Exclude the provided attribute names
            project_attrs = [name for name, _ in pairs if name not in attributes]
        else:
            # Just use the provided attributes
            project_attrs = list(attributes)

        attributes_s = (snake(s) for s in project_attrs)
        cmd = f"set {_relation} [{cls._cmd_project(relation=snake(relation), attributes=attributes_s)}]"
        result = Database.execute(db=db, cmd=cmd)
        if svar_name:  # Save the result using the supplied session variable name
            cls.set_var(db=db, name=svar_name)
        return cls.make_pyrel(result)

    @classmethod
    def make_comparison(cls, attr_name: str, values: set | str) -> str:
        if isinstance(values, set):
            vmatch = [f"[string match {{{v}}} [tuple extract $t {attr_name}]]" for v in values]
            return '(' + ' || '.join(vmatch) + ')'

        # There's only one value
        return f"[string match {{{values}}} [tuple extract $t {attr_name}]]"

    @classmethod
    def summarizeby(cls, db: str, relation: str, attrs: List[str], sum_attr: Attribute, op='count',
                    svar_name: Optional[str] = None) -> RelationValue:
        """
        DEPRECATED

        Only one summarization operation supported by PyRAL at the moment - count
        :param db: DB session name
        :param relation:
        :param attrs:
        :param sum_attr:
        :param op:
        :param svar_name:
        :return:

        From TclRAL man page:
        --
        The summarizeby subcommand is a more convenient form of summarize where the per relation is a projection of
        the relation value that is to be summarized. Rather than supplying a per relation, instead a list of
        attributes is given by the attrList argument and relationValue is projected on those attributes and
        used as the per relation. The arguments and results are otherwise the same as for the summarize command.

        TclRAL syntax:
        relation summarizeby relationValue attrList relationVarName attr type expression ?attr type expression ...?

        TclRAL command example:
        % relformat [relation summarizeby $OWNERSHIP Acquired s NumAcquired int {[relation cardinality $s]}]

        PyRal implements this in limited form for now.  Only the cardinality operations is supported.
        Given this input to PyRAL:

            Relation.summarizeby(db=stdb, relation='Region', attrs=['Data_box', 'Title_block_pattern'],
                                 sum_attr=Attribute(name='Qty', type='int'), svar_name="Number_of_regions")

            We get the TclRAL command:
                relation summarizeby ${Region} {Data_box Title_block_pattern} s Qty int {[relation cardinality $s]}

        Applied to this example relation:

        -- Region --
        +------------+-----------------------+---------------+
        |   Data_box | Title_block_pattern   |   Stack_order |
        +============+=======================+===============+
        |          3 | Complex               |             1 |
        |          3 | Complex               |             2 |
        |          3 | SE Simple             |             1 |
        |          5 | SE Simple             |             1 |
        |          6 | SE Simple             |             1 |
        |          6 | SE Simple             |             2 |
        |          7 | SE Simple             |             1 |
        |          7 | SE Simple             |             2 |
        +------------+-----------------------+---------------+

        We get the following result which tells us the number of Regions per Data Box+Title_block_pattern

        -- Number_of_regions --
        +------------+-----------------------+-------+
        |   Data_box | Title_block_pattern   |   Qty |
        +============+=======================+=======+
        |          3 | Complex               |     2 |
        |          3 | SE Simple             |     1 |
        |          5 | SE Simple             |     1 |
        |          6 | SE Simple             |     2 |
        |          7 | SE Simple             |     2 |
        +------------+-----------------------+-------+

        --
        """
        cmd = (f"set {_relation} [relation summarizeby ${{{relation}}} {{{' '.join(attrs)}}} s "
               f"{sum_attr.name} {sum_attr.type} {{[relation cardinality $s]}}]")
        result = Database.execute(db=db, cmd=cmd)
        if svar_name:  # Save the result using the supplied session variable name
            cls.set_var(db=db, name=svar_name)
        return cls.make_pyrel(result)

    @classmethod
    def rank(cls, db: str, order: Order, sort_attr_name: str, rank_attr_name: str = _RANK, relation: str = _relation,
             svar_name: Optional[str] = None) -> RelationValue:
        """
        TclRAL documentation and syntax:

            relation rank relationValue ?-ascending | -descending? rankAttr newAttr

        The rank subcommand returns a new relation whose heading is the same as relationValue
        extending by an attribute named newAttr. The type of newAttr will be int and its value
        will be set to the number of tuples in relationValue where the value of rankAttr is
        less than or equal to (?-descending?) or greater than or equal to (?-ascending?) that of
        the given tuple. The default ranking is -ascending.

        The type of rankAttr must be int, double, or string.

        The rank command is useful when it is desirable to limit the number of tuples in the result.

        PyRAL example:

            result = Relation.rank(db=ev, relation="shafts_rv", sort_attr_name="Speed", order=Order.DESCENDING)

        :param db: DB session name
        :param order: ascending or descending (no default ordering)
        :param sort_attr_name: The values of this attribute will be sorted (TclRAL rankAttr)
        :param rank_attr_name: The name of the added rank number attribute (TclRAL newAttr)
        :param relation: Relation to be sorted
        :param svar_name: An optional session variable that holds the result
        :return: Relation with added rank_attr_name
        """
        cmd = f"set {_relation} [relation rank ${{{relation}}} -{order.value}{{{sort_attr_name}}}{{{rank_attr_name}}}]"

        result = Database.execute(db=db, cmd=cmd)
        if svar_name:
            cls.set_var(db=db, name=svar_name)
        return cls.make_pyrel(result)

    @classmethod
    def tag(cls, db: str, tag_attr_name: str = _TAG, sort_attrs: Tuple[str, ...] = None,
            order: Order = Order.ASCENDING, relation: str = _relation,
            svar_name: Optional[str] = None) -> RelationValue:
        """
        TclRAL documentation:

            relation tag relationValue attrName ?-ascending | -descending sort-attr-list? ?-within attr-list?

        The tag subcommand creates a new relation which has the same heading as relationValue extended by a
        new attribute named attrName. The type of attrName will be int and will have the values between 0 and the
        cardinality of relationValue minus one. The tuples in relationValue will be extended in either ascending or
        descending order of the sort-attr-list.

        If no sort-attr-list argument is given, then the tagging order is arbitrary.

        If the ?-within? argument is given then the values of attrName attribute will be unique within the subset
        of tuples which match the values of attr-list. The tag command is useful when a full ordering needs to be
        placed on a relation. For example, tagging a relation will allow projecting both the tag and another
        attribute without losing any values.

        PyRAL implements this partially for now and the -within option is not yet supported.

        :param sort_attrs:
        :param order:
        :param db:
        :param tag_attr_name:
        :param relation:
        :param svar_name:
        :return:
        """
        do_sort = "" if not sort_attrs else f" -{order.value} {' '.join(sort_attrs)} "
        cmd = f"set {_relation} [relation tag ${{{relation}}} {{{tag_attr_name}}} {do_sort}]"
        result = Database.execute(db=db, cmd=cmd)
        if svar_name:
            cls.set_var(db=db, name=svar_name)
        return cls.make_pyrel(result)

    @classmethod
    def rank_restrict(cls, db: str, attr_name: str, extent: Extent, card: Card, relation: str = _relation,
                      svar_name: Optional[str] = None) -> RelationValue:
        """
        Our goal here is to order the tuples of a relation on some attribute and then select the
        furthest extent, either greatest or least.

        For example, we might want to select the highest flying aircraft.

        We do this by ranking all aircraft tuples according to altitude in descending order.

        Then we select all those of ranking 1. Since multiple aircraft might be flying at the same highest
        altitude, there may be multiple ranked as 1. And if there are no aircraft instances, we'll find
        no highest flying aicraft.

        If the user specifies the ALL cardinality, they could obtain multiple aircraft tuples at the same
        altitude. However, the ONE cardinality specifies that only one will be selected and the user cannot
        choose which particular tuple is selected.

        :param db: DB session name
        :param attr_name: Tuples are ordered based on values of this attribute
        :param extent: greatest or least
        :param card: one or all
        :param relation: Selection is on tuples of this relation
        :param svar_name: Relation result is stored in this optional TclRAL variable for subsequent operations to use
        :return: The tuple or tuples at the same extent
        """
        order = Order.DESCENDING if extent == Extent.GREATEST else Order.ASCENDING
        if card == Card.ALL:
            Relation.rank(db=db, order=order, sort_attr_name=attr_name, relation=relation)
            R = f"{_RANK}:1"
            Relation.restrict(db=db, restriction=R)
            return Relation.project(db=db, attributes=(_RANK,), exclude=True, svar_name=svar_name)
        else:  # Card must be ONE
            Relation.tag(db=db, order=order, sort_attrs=(attr_name,), relation=relation)
            R = f"{_TAG}:0"
            Relation.restrict(db=db, restriction=R)
            return Relation.project(db=db, attributes=(_TAG,), exclude=True, svar_name=svar_name)

    @classmethod
    def restrict(cls, db: str, restriction: Optional[str] = None, relation: str = _relation,
                  svar_name: Optional[str] = None) -> RelationValue:
        """
        Here we select zero or more tuples that match the supplied criteria.

        In relational theory this is known as a restriction operation.

        TclRAL syntax:
            relation restrictwith <relationValue> <expression>

        The most common usage scenario is to select on a single identifier attribute value.

            R = f"ID:S1"
            result = Relation.restrict(db=ev, relation="shafts_rv", restriction=R, svar_name="restriction")

            -- restriction --
            +------+---------+--------------+
            | ID   |   Speed | In_service   |
            +======+=========+==============+
            | S1   |      31 | True         |
            +------+---------+--------------+

            This yields the TclRAL statement

            relation restrictwith ${shafts_rv} {[string match {S1} $ID]}

            So we have the command, a relation variable, and the string match.
            The string match arguments are the value to match and the name of the attribute as a tcl variable

        If there is any whitespace in the value, use <> brackets to enclose it. For example:

            R = f"Level_name:<3rd Floor>"

        Note that the attribute names must be specified in snake case. Relation names can have spaces, but are
        automatically converted to snake case in the generated TclRAL. It's a bit trickier to do this with all the
        attribute names since they are embedded in the restriction expression, so we put that burden on
        the user.

        Note that the : symbol is shorthand for matching and only works with strings. The == operator can
        only be used to match numeric values.

        You can match multiple values using the ', ' to AND them together. Here is the supplied PyRAL expression
        and generated TclRAL expression:

            PyRAL: R = f"ID:<{i}>, In_service:<{True}>"
            TclRAL: {[string match {S1} $ID] && [string match {True} $In_service]}

        Here we use i to specify the "S1" ID value using the python formatted string {} brackets.
        The brackets around True are necessary to ensure a boolean value is inserted and not the "True" string.
        (though the string value would work as well since Tcl represents a boolean value as a truthy string)

        Numeric comparisons can be supplied using the usual operators with s set to the integer 14:

            PyRAL: R = f"Speed > {s}"
            TclRAL: {[expr {$Speed > 14}]}

        In a numeric comparison you do not need to surround the numeric value with <> brackets.

        Finally, you can express more complex logic using nested parentheses and the logic operators
        AND, OR, NOT as follows:

            PyRAL: R = f"ID:<{v}> OR (In_service:<{True}> AND Speed > {s})"
            TclRAL: {[string match {S1} $ID] || ([string match {True} $In_service] && [expr {$Speed > 31}])}

        :param db: DB session name
        :param relation: Name of a relation variable where the operation is applied
        :param restriction: A string in Scrall notation that specifies the restriction criteria
        :param svar_name: An optional session variable that holds the result
        :return: The TclRAL string result representing the restricted tuple set
        """
        relation_s = snake(relation)
        if not restriction:
            cmd = f"set {_relation} [set {relation_s}]"
        else:
            # Handle arithmetic comparisons like Speed > 14
            restrict_tcl = re.sub(
                pattern=r'(\w+)\s*(==|!=|>=|<=|<|>)\s*(-?\d+(?:\.\d+)?)',
                repl=r'[expr [tuple extract $t \1] \2 \3]',
                string=restriction
            )

            # Handle attr:<value> form for string match
            restrict_tcl = re.sub(
                pattern=r'([\w_]+):<([^>]*)>',
                repl=r'[string match {\2} [tuple extract $t \1]]',
                string=restrict_tcl
            )

            # Handle attr:value form for string match (NEW addition)
            restrict_tcl = re.sub(
                pattern=r'([\w_]+):([^\s<>()&|!]+)',
                repl=r'[string match {\2} [tuple extract $t \1]]',
                string=restrict_tcl
            )

            # Convert boolean logic operators and NOT
            restrict_tcl = restrict_tcl.replace(' OR ', ' || ') \
                .replace(', ', ' && ') \
                .replace(' AND ', ' && ') \
                .replace('NOT ', '!')

            rexpr = f"{{{restrict_tcl}}}"
            cmd = f"set {_relation} [relation restrict ${{{relation_s}}} t {rexpr}]"

        result = Database.execute(db=db, cmd=cmd)
        if svar_name:
            cls.set_var(db=db, name=svar_name)
        return cls.make_pyrel(result)

