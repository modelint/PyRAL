"""
relation.py – Operations on relations
"""

# System
import logging
import re
from tabulate import tabulate
from typing import List, Optional, Dict, Tuple
from collections import namedtuple
from collections.abc import Sequence

# PyRAL
from pyral.rtypes import *
from pyral.database import Database
from pyral.exceptions import TclRALException

_logger = logging.getLogger(__name__)

# If we want to apply successive (nested) operations in TclRAL we need to have the result
# of each TclRAL command saved in tcl variable. So each time we execute a command that produces
# a relation result we save it. The variable name is chosen so that it shouldn't conflict with
# any user relvars. Do not ever use the name below as one of your user relvars!
# For any given command, if no relvar is specified, the previous relation result is assumed
# to be the input.
_relation = r'^relation'  # Name of the latest relation result. Carat prevents name collision
_RANK = "_rank"  # Default name of the rank attribute added by extension using the rank command
_TAG = "_tag"  # Default name of the tag attribute added by the tag command
session_variable_names = set()  # Maintain a list of temporary variable names in use

def _next_tuple_var(c: str) -> str:
    """
    For TclRAL commands that use temporary embedded tuple variables, we keep them unique by
    generating in alphabetic sequence.

    Args:
        c: Character last used.

    Returns:
        Next character to be used as a tuple variable name.
    """
    return chr(ord(c) + 1)

def _shield_braces(text: str) -> tuple[str, dict[str, str]]:
    """
    Temporarily replaces all brace-enclosed substrings (e.g., {NOT REQUESTED})
    with unique placeholder tokens, so that logic substitutions like 'AND'
    or 'NOT' do not affect content inside braces.

    Args:
        text: The input string containing brace-enclosed segments.

    Returns:
        A tuple of:
            - The input string with brace-enclosed segments replaced by tokens.
            - A dictionary mapping token keys back to their original substrings.
    """
    protected = {}

    def replacer(match):
        key = f"__PROTECTED_{len(protected)}__"
        protected[key] = match.group(0)  # include the braces
        return key

    result = re.sub(r'\{[^}]*\}', replacer, text)
    return result, protected


def _unshield_braces(text: str, protected: dict[str, str]) -> str:
    """
    Restores previously shielded brace-enclosed substrings back into the text.

    Args:
        text: The string containing placeholder tokens.
        protected: A mapping from token keys to their original brace-wrapped values.

    Returns:
        The original string with all placeholders replaced by their corresponding
        brace-enclosed content.
    """
    for key, value in protected.items():
        text = text.replace(key, value)
    return text


class Relation:
    """
    A relational value
    """

    @classmethod
    def declare_rv(cls, db: str, owner: str, name: str) -> str:
        """
        Add a relational variable to the db session managed by the specified owner.

        Args:
            db: Name of an active database session.
            owner: Name of the client specified owner responsible for managing this variable.
            name: Name of the variable.

        Returns:
            The name of the relational variable.
        """
        owner_s = snake(owner)
        name_s = snake(name)
        # Verify that db session exists
        if db not in Database.sessions:
            raise KeyError(f"Database session '{db}' has not been initialized.")

        db_rvs = Database.rv_names.setdefault(db, {})
        owner_rvs = db_rvs.setdefault(owner_s, set())

        if name_s in owner_rvs:
            raise KeyError(f"Relational variable {name_s} already defined for owner {owner_s}")

        owner_rvs.add(name_s)
        # We replace any whitespace with underscores before passing into TclRAL
        return f"{owner_s}__{name_s}"

    @classmethod
    def free_rvs(cls, db: str, owner: str, names: tuple[str, ...] = (), exclude: bool = False):
        """
        Unset relation variable names declared by the owner.

        Args:
            db: Database session name.
            owner: Name of the owner who declared the relational variables.
            names: Names to include or exclude depending on ``exclude``.
            exclude: If True, keep the listed names and delete all others.
                If False, delete only the listed names. If names is empty, delete all.
        """
        # Remove spaces from owner and names
        owner_s = snake(owner)
        names_s = [snake(s) for s in names]
        try:
            owner_rvs = Database.rv_names[db][owner_s]
        except KeyError:
            _logger.warning(f"No such owner '{owner_s}' declared in session '{db}'")
            return

        names_to_remove = (
            owner_rvs - set(names_s) if exclude and names_s
            else set(names_s) if names_s
            else owner_rvs
        )

        for name in names_to_remove:
            cmd = f"unset {owner_s}__{name}"
            try:
                Database.execute(db=db, cmd=cmd)
            except TclRALException:
                pass

        # Remove updated set of RVs or delete owner entry entirely if now empty
        remaining = owner_rvs - names_to_remove
        if remaining:
            Database.rv_names[db][owner_s] = remaining
        else:
            Database.rv_names[db].pop(owner_s, None)
            if not Database.rv_names[db]:
                Database.rv_names.pop(db, None)

    @classmethod
    def summarize(cls, db: str, per_attrs: Tuple[str, ...], summaries: Tuple[SumExpr], relation: str = _relation,
                  svar_name: Optional[str] = None) -> RelationValue:
        """
        Full implementation of summarize/summarizeby.

        Args:
            db: DB session name.
            per_attrs: Tuple of attribute names to summarize per.
            summaries: Tuple of SumExpr named tuples defining the summary attributes and expressions.
            relation: The relation to summarize.
            svar_name: Relation result is stored in this optional TclRAL variable for subsequent operations to use.

        Returns:
            Resulting relation as a PyRAL relation value.
        """
        sum_expr_strings = [f"{s.attr.name} {s.attr.type} {{[{s.expr}]}}" for s in summaries]
        summaries_clause = ' '.join(sum_expr_strings)
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

        For example, these three input cmd_strings::

            0 'relation is ${^relation} subsetof $xactions'
            1 'relation project ${^relation} From_action'
            2 'relation join ${s} $required_inputs'

        Collapse into this returned command string::

            'relation is [relation project [relation join ${s} $required_inputs] From_action] subsetof $xactions'

        Expression expansion is indicated by the _relation variable which contains the TclRAL variable name
        representing the output yielded by the previously executed command. As of this writing, the variable name
        is: ${^relation}

        The first command in the list contains the text _relation value somewhere in the string.

        We need to replace this value with the next command string surrounded in brackets [<command_string>].
        But, unless that command is the last one in the list, it too will contain the _relation text,
        requiring further expansion.

        So we descend recursively until we hit the bottom command. There we simply return the command text surrounded
        in brackets replacing as we go for each _relation occurrence moving back up the stack.

        Args:
            cmd_strings: A list of TclRAL command strings in reverse nesting order.

        Returns:
            The fully expanded TclRAL command with all _relation occurrences replaced.
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
        Builds a nested TclRAL expression command for use with summarize.

        For example, let's say we want to combine this list of PyRAL Relation commands
        into a single TclRAL expression::

            Relation.join(db=fdb, rname2="required_inputs", rname1="s")
            Relation.project(db=fdb, attributes=("From_action",))
            Relation.set_compare(db=fdb, rname2="xactions", op=SetOp.subset)

        We will receive these as a list of corresponding namedtuples::

            JoinCmd(rname1="s", rname2="required_inputs", attrs=None),
            ProjectCmd(attributes=("From_action",), relation=None),
            SetCompareCmd(rname2="xactions", op=SetOp.subset, rname1=None)

        Args:
            commands: A list of relation command namedtuples in execution order.

        Returns:
            A fully nested TclRAL command string.
        """

        # Here is each named tuple defining a Relation method that can be built into an expression
        relation_method = {
            "SetCompareCmd": lambda c: cls._cmd_set_compare(rname1=c.rname1, rname2=c.rname2, op=c.op),
            "ProjectCmd": lambda c: cls._cmd_project(db=c.db, relation=c.relation, attributes=c.attributes),
            "JoinCmd": lambda c: cls._cmd_join(rname1=c.rname1, rname2=c.rname2, attrs=c.attrs),
            "SemiJoinCmd": lambda c: cls._cmd_semijoin(rname1=c.rname1, rname2=c.rname2, attrs=c.attrs),
            "RestrictCmd": lambda c: cls._cmd_restrict(relation=c.relation, restriction=c.restriction),
            "CardinalityCmd": lambda c: cls._cmd_cardinality(rname=c.rname),
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
    def _cmd_cardinality(cls, rname: Optional[str] = None) -> str:
        if rname is None:
            rname = _relation
        cmd = f'relation cardinality ${{{snake(rname)}}}'
        return cmd

    @classmethod
    def _cmd_set_compare(cls, rname2: str, op: SetOp, rname1: Optional[str] = None) -> str:
        if rname1 is None:
            rname1 = _relation
        return f'relation is ${{{snake(rname1)}}} {op.value} ${snake(rname2)}'

    @classmethod
    def _cmd_extend(cls, attrs: dict[str, str | int | float | bool], relation) -> str:
        if relation is None:
            relation = _relation

        extensions = []
        for n, v in attrs.items():
            cmd_prefix = f"{{{snake(n)}}} "  # TclRAL tuple var name 't' and attr name
            python_type_name = type(v).__name__
            attr_type = f"{tcl_type[python_type_name]}"  # attr type name
            val_str = f'{{{{{v}}}}}' if attr_type == "string" else str(v)
            item_str = cmd_prefix + attr_type + " " + val_str
            extensions.append(item_str)

        ext_str = ' '.join(extensions)
        r = f"relation extend ${{{snake(relation)}}} e {ext_str}"
        return r

    @classmethod
    def _cmd_restrict(cls, restriction: Optional[str] = None, relation: Optional[str] = None) -> str:
        if relation is None:
            relation_s = _relation
        else:
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

            # Shield all {...} blocks so that we don't do boolean substitution inside
            restrict_tcl, protected_map = _shield_braces(restrict_tcl)

            # Convert boolean logic operators and NOT
            restrict_tcl = restrict_tcl.replace(' OR ', ' || ') \
                .replace(', ', ' && ') \
                .replace(' AND ', ' && ') \
                .replace('NOT ', '!')

            # Restore the protected {...} blocks now that the boolean substitution has completed
            restrict_tcl = _unshield_braces(restrict_tcl, protected_map)

            rexpr = f"{{{restrict_tcl}}}"

            cmd = f"relation restrict ${{{relation_s}}} t {rexpr}"
        return cmd

    @classmethod
    def _cmd_project(cls, db: str, attributes: Sequence[str], exclude: bool = False, relation: str = _relation) -> str:
        if relation is None:
            relation_s = _relation
        else:
            relation_s = snake(relation)

        # Create a list of attributes to project by inclusion or exclusion
        if exclude:
            attr_types = Relation.heading(db=db, relation=relation_s)
            tokens = attr_types.split()
            pairs = zip(tokens[::2], tokens[1::2])
            project_attrs = [name for name, _ in pairs if name not in attributes]
        else:
            project_attrs = list(attributes)

        attributes_s = ' '.join(snake(s) for s in project_attrs)
        cmd = f"relation project ${{{relation_s}}} {attributes_s}"
        return cmd

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
    def _cmd_semiminus(cls, attrs, rname1: Optional[str] = None, rname2: Optional[str] = None) -> str:
        if rname1 is None:
            rname1 = _relation
        if rname2 is None:
            rname2 = _relation
        using = f" -using {cls.make_attr_list(attrs)}" if attrs else ""
        return f"relation semiminus ${{{snake(rname1)}}} ${{{snake(rname2)}}}{using}"

    @classmethod
    def _cmd_semijoin(cls, rname2: Optional[str], attrs, rname1: Optional[str] = None) -> str:
        if rname1 is None:
            rname1 = _relation
        if rname2 is None:
            rname2 = _relation
        using = f" -using {cls.make_attr_list(attrs)}" if attrs else ""
        return f"relation semijoin ${{{snake(rname1)}}} ${{{snake(rname2)}}}{using}"

    @classmethod
    def set_compare(cls, db: str, rname2: str, op: SetOp, rname1: str = _relation) -> bool:
        """
        Perform a set comparison between two relations.

        Args:
            db: DB session name.
            rname1: If not specified, the previous relation result is used.
            rname2: Each rname must have the same header.
            op: A SetOp enumeration element defined in rtypes.py.

        Returns:
            The boolean result of the set operation.
        """
        cmd = cls._cmd_set_compare(rname1=snake(rname1), rname2=snake(rname2), op=op)
        result = bool(int(Database.execute(db=db, cmd=cmd)))
        return result

    @classmethod
    def create(cls, db: str, attrs: List[Attribute], tuples: List[namedtuple] | List[tuple],
               svar_name: Optional[str] = None) -> RelationValue:
        """
        Create a relation.

        Args:
            db: DB session name.
            attrs: A tuple of attributes (name, type) pairs.
            tuples: A list of tuples named such that the attributes exactly match the relvar header.
            svar_name: Relation result is stored in this optional TclRAL variable for subsequent operations to use.

        Returns:
            Resulting relation as a PyRAL relation value.
        """
        h = header(attrs)
        if tuples and isinstance(tuples[0], tuple) and hasattr(tuples[0], '_fields'):
            b = body(tuples)
        else:
            b = body_tuple(attrs=attrs, tuples=tuples)

        cmd = f'set {_relation} [relation create {h} {b}]'
        result = Database.execute(db=db, cmd=cmd)
        if svar_name:  # Save the result using the supplied session variable name
            cls.set_var(db=db, name=svar_name)
        return cls.make_pyrel(result)

    @classmethod
    def build_select_expr(cls, selection: str) -> str:
        """
        Convert a Scrall style select expression to an equivalent Tcl string match expression.

        For now we only support an and'ed list of direct string matches in the format::

            attr1:str1; attr2:str2, ...

        With the assumption that we would like to select each tuple where::

            attr1 == str1 AND attr2 == str2 ...

        We'll convert this to a Tcl expression like this::

            {[string match str1 $attr1] && [string match str2 $attr2] ...}

        Note that this only works for the TclRAL relation restrictwith command and not the
        relation restrict command. But that should suffice for our purposes.

        Once our Scrall parser is ready, we can expand the functionality further.

        Args:
            selection: The Scrall style select expression.

        Returns:
            The Tcl expression string.
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

        Args:
            db: DB session name.
            name: The variable name (must be a legal Tcl variable name).
        """
        session_variable_names.add(name)
        Database.sessions[db].eval(f"set {name} ${{{_relation}}}")

    @classmethod
    def make_attr_list(cls, attrs: Dict[str, str]) -> str:
        """
        Makes a TclRAL attrList to be inserted in a command.

        Args:
            attrs: A dictionary mapping attribute names to their renamed equivalents.

        Returns:
            A TclRAL formatted attribute list string.
        """
        attr_list = "{"
        for k, v in attrs.items():
            attr_list += f"{snake(k)} {v} "
        return attr_list[:-1] + "}"

    @classmethod
    def semijoin(cls, db: str, rname2: str = _relation, attrs: Optional[Dict[str, str]] = None, rname1: str = _relation,
                 svar_name: Optional[str] = None) -> RelationValue:
        """
        Perform a semi join on two relations using an optional attribute mapping. If no attributes are specified,
        the semi-join is performed on same named attributes.

        From the TclRAL man page: The semijoin subcommand computes the join of relationValue1 and relationValue2
        but eliminates all of the attributes of relationValue1 (or alternatively speaking, projecting all
        attributes of relationValue2). The returned relation has a heading the same as relationValue2 and a body
        consisting of those tuples in relationValue2 that would have been included in the natural join with
        relationValue1.

        Args:
            db: DB session name.
            rname1: Name of one relvar to join.
            rname2: Name of the other relvar.
            attrs: Dictionary in format { r1.attr_name: r2.attr_name, ... }.
            svar_name: Relation result is stored in this optional TclRAL variable for subsequent operations to use.

        Returns:
            Resulting relation as a PyRAL relation value.
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

        Args:
            db: DB session name.
            rname1: Name of one relvar to join.
            rname2: Name of the other relvar.
            attrs: Dictionary in format { r1.attr_name: r2.attr_name, ... }.
            svar_name: Relation result is stored in this optional TclRAL variable for subsequent operations to use.

        Returns:
            Resulting relation as a PyRAL relation value.
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
        Given an input relation, rename one or more attributes from old to new names. This is useful when you want
        to join two relvars on attributes with differing names.

        Note: TclRAL's join command also provides an option to specify multiple renames as part of a join,
        so there may not always be a need for this method. It does at least handle the single rename case cleanly.

        In SM xUML, it is common for an attribute with one name to reference another attribute of the same
        type but a different name, e.g. Employee_ID -> Manager.

        We often need to rename multiple attributes before performing a join, so the single attribute rename
        operation provided by TclRAL is executed once for each element of the names dictionary.

        TclRAL rename syntax::

            relation rename <relationValue> ?oldname newname ...?

        Multiple rename example in TclRAL::

            relation rename ${Attribute_Reference} To_attribute Name
            relation rename ${^relation} To_class Class

        Generated from the PyRAL input::

            relation: 'Attribute_Reference'
            names: {'To_attribute': 'Name', 'To_class': 'Class'}

        Args:
            db: DB session name.
            relation: The relation to rename.
            names: Dictionary in format { old_name: new_name }.
            svar_name: Name of a TclRAL session variable named for future reference.

        Returns:
            Resulting relation as a PyRAL relation value.
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

        The TclRAL syntax is::

            relation intersect <relationValue1> <relationValue2>

        Args:
            db: DB session name.
            rname1: Name of the first relation.
            rname2: Name of the second relation.
            svar_name: Relation result is stored in this optional TclRAL variable for subsequent operations to use.

        Returns:
            Resulting intersection relation as a PyRAL relation value.
        """
        cmd = f'set {_relation} [relation intersect ${{{rname1}}} ${rname2}]'
        result = Database.execute(db, cmd)
        if svar_name:  # Save the result using the supplied session variable name
            cls.set_var(db, svar_name)
        return cls.make_pyrel(result)

    @classmethod
    def compare(cls, db: str, op: str, rname2: str, rname1: str = _relation) -> bool:
        """
        Returns the boolean result of comparing two relations with the given operator.

        Each relation must be of the same type (same header).

        The TclRAL syntax is::

            relation is <relationValue1> <op> <relationValue2>

        Args:
            db: DB session name.
            op: Comparison operation string (e.g. 'equal', 'subsetof').
            rname1: Name of the first relation.
            rname2: Name of the second relation.

        Returns:
            Boolean result of the comparison operation.
        """
        cmd = f'relation is ${{{snake(rname1)}}} {op} ${snake(rname2)}'
        result = bool(int(Database.execute(db=db, cmd=cmd)))
        return result

    @classmethod
    def cardinality(cls, db: str, rname: str = _relation) -> int:
        """
        Returns the number of tuples contained in the body of the relation.

        Args:
            db: DB session name.
            rname: The tuples in the body of this relation are counted.

        Returns:
            The number of tuples in the relation.
        """
        cmd = f"set {{{_relation}}} [{cls._cmd_cardinality(rname=rname)}]"
        result = int(Database.execute(db=db, cmd=cmd))
        return result

    @classmethod
    def subtract(cls, db: str, rname2: str = _relation, rname1: str = _relation, svar_name: Optional[str] = None
                 ) -> RelationValue:
        """
        Returns the set difference between two relations using the TclRAL minus command.

        Each relation must be of the same type (same header) as will the result.

        The body of the result consists of those tuples present in r1 but not present in r2.

        Relational subtraction is not commutative so the order of the r1 and r2 arguments is significant.

        The TclRAL syntax is::

            relation minus <relationValue1> <relationValue2>

        TclRAL example taken from the lineage.py Derive method where a set of all classes playing one or more
        subclass roles subtracts all classes playing superclass roles to obtain a set of leaf classes::

            relation minus $subs $supers

        Generated from the following PyRAL input::

            rname1: subs
            rname2: supers

        Args:
            db: The TclRAL session.
            rname1: Relation from which rname2 is subtracted.
            rname2: Relation to subtract.
            svar_name: Relation result is stored in this optional TclRAL variable for subsequent operations to use.

        Returns:
            Relation result of the subtraction.
        """
        cmd = f'set {_relation} [relation minus ${{{rname1}}} ${{{rname2}}}]'
        result = Database.execute(db, cmd)
        if svar_name:  # Save the result using the supplied session variable name
            cls.set_var(db, svar_name)
        return cls.make_pyrel(result)

    @classmethod
    def get_rval_string(cls, db: str, variable_name: str) -> str:
        """
        Obtain a relation from a TclRAL variable.

        Args:
            db: DB session name.
            variable_name: Name of a variable containing a relation defined in that session.

        Returns:
            TclRAL string representing the relation value.
        """
        return Database.execute(db, cmd=f"set {variable_name}")

    @classmethod
    def make_pyrel(cls, relation: str, name: str = _relation) -> RelationValue:
        """
        Take a relation obtained from TclRAL and convert it into a pythonic relation value.
        A RelationValue is a named tuple with a header and a body component.
        The header component will be a dictionary of attribute name keys and type values.
        The body component will be a list of relational tuples each defined as a dictionary
        with a key matching some attribute of the header and a value for that attribute.

        Args:
            relation: A TclRAL string representing a relation.
            name: An optional relvar name.

        Returns:
            A RelationValue constructed from the provided relation string.
        """
        # First check for the dee/dum edge cases
        if relation.strip() == '{} {}':
            # Tabledum (DUM), no attributes and no tuples (relational false value)
            return RelationValue(name=name, header={}, body=[])
        if relation.strip() == '{} {{}}':
            # Tabledum (DEE), no attributes and one empty tuple (relational true value)
            return RelationValue(name=name, header={}, body=[{}])

        # Going forward we can assume that there is at least one attribute and zero or more tuples
        h, b = relation.split('}', 1)  # Split at the first closing bracket to obtain header and body strings

        # Construct the header dictionary
        h_items = h.strip('{').split()  # Remove the open brace and split on spaces
        header = dict(zip(h_items[::2], h_items[1::2]))

        # Construct the body list
        body = b[2:-2].split('} {')
        body[0] = body[0].lstrip('{')  # Remove preceding bracket from the first tuple

        value_pattern = r"(.*)"
        tuple_pattern = ""
        for a in header.keys():
            tuple_pattern += f"{a} {value_pattern} "
        tuple_pattern = tuple_pattern.rstrip(' ')

        # Handle case where there are zero body tuples
        at_least_one_tuple = b.strip('{} ')
        if not at_least_one_tuple:
            return RelationValue(name=name, header=header, body={})

        # There is at least one body tuple
        if len(header) > 1:
            b_rows = [[f.strip('{}') for f in re.findall(tuple_pattern, row)[0]] for row in body]
        else:
            b_rows = [[re.findall(tuple_pattern, row)[0].strip('{}')] for row in body]

        body = [dict(zip(header.keys(), r)) for r in b_rows]
        rval = RelationValue(name=name, header=header, body=body)
        return rval

    @classmethod
    def printc(cls, db: str, variable_name: str = _relation, table_name: Optional[str] = None,
              printout: bool = True):
        """
        Given the name of a TclRAL relation variable, obtain its value and print it as a table.

        Args:
            db: DB session name.
            variable_name: Name of the TclRAL variable to print, also used to name the table if no table_name.
            table_name: If supplied, this name is used instead of the variable name to name the printed table.
            printout: Print to console if true.
        """
        rval = cls.make_pyrel(relation=cls.get_rval_string(db=db, variable_name=snake(variable_name)),
                              name=table_name if table_name else variable_name)
        cls.relformat(rval, printout)

    @classmethod
    def print(cls, db: str, variable_name: str = _relation, table_name: Optional[str] = None,
              printout: bool = True) -> str:
        """
        Given the name of a TclRAL relation variable, obtain its value and print it as a table.

        Args:
            db: DB session name.
            variable_name: Name of the TclRAL variable to print, also used to name the table if no table_name.
            table_name: If supplied, this name is used instead of the variable name to name the printed table.
            printout: Print to console if true.

        Returns:
            The formatted table string.
        """
        rval = cls.make_pyrel(relation=cls.get_rval_string(db=db, variable_name=snake(variable_name)),
                              name=table_name if table_name else variable_name)
        return cls.relformat(rval, printout)

    @classmethod
    def relformat(cls, rval: RelationValue, printout: bool = True) -> str:
        """
        Formats the PyRAL relation into a table and prints it using the imported tabulation module.

        Args:
            rval: A PyRAL relation value.
            printout: Print to console if true.

        Returns:
            The formatted table string including the table header line.
        """
        tablename = rval.name if rval.name else '<unnamed>'
        tableheader = f"[-- {tablename} --]"
        if printout:
            print(tableheader)
        attr_names = list(rval.header.keys())
        brows = [list(row.values()) for row in rval.body]
        table_text = tabulate(tabular_data=brows, headers=attr_names, tablefmt="outline")
        if printout:
            print(table_text)
        return f"{tableheader}\n{table_text}"

    @classmethod
    def union(cls, db: str, relations: Tuple[str, ...], svar_name: Optional[str] = None) -> RelationValue:
        """
        Returns the set union of two or more relations.
        All relations must be of the same type.

        The result relation has a heading that is the same as any of the arguments and
        has a body consisting of all tuples present in any of the relationValue arguments.

        Since the union operation is both associative and commutative, the order of the
        relationValue arguments has no effect on the result.

        Args:
            db: DB session name.
            relations: A tuple of relation variable names to union together.
            svar_name: Relation result is stored in this optional TclRAL variable for subsequent operations to use.

        Returns:
            Resulting relation as a PyRAL relation value.
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
        Implements the relational divide operation.

        The headings of dividend and divisor must be disjoint and the heading of mediator must be
        the union of the dividend and divisor headings.

        The returned result is a new relation that has the same heading as dividend and contains
        all the tuples from dividend whose corresponding tuples in mediator include all the tuples in divisor.
        Stated another way, the result is the maximal set of tuples from dividend whose
        Cartesian product with divisor is completely contained in mediator.

        Args:
            db: DB session name.
            dividend: The dividend relation name.
            divisor: The divisor relation name.
            mediator: The mediator relation name.
            svar_name: Relation result is stored in this optional TclRAL variable for subsequent operations to use.

        Returns:
            Resulting relation as a PyRAL relation value.
        """
        cmd = f'set {_relation} [relation divide ${dividend} ${divisor} ${mediator}]'
        result = Database.execute(db=db, cmd=cmd)
        if svar_name:  # Save the result using the supplied session variable name
            cls.set_var(db=db, name=svar_name)
        return cls.make_pyrel(result)

    @classmethod
    def raw(cls, db: str, cmd_str: str, relation: str = _relation,
            svar_name: Optional[str] = None) -> RelationValue | None:
        """
        Passes tcl cmd text straight through, but uses the variable and relation
        naming mechanism to pipeline input and output like all other commands.

        Args:
            db: DB session name.
            cmd_str: A raw TclRAL command string to evaluate.
            relation: Input relation variable name.
            svar_name: Relation result is stored in this optional TclRAL variable for subsequent operations to use.

        Returns:
            Resulting relation as a PyRAL relation value.
        """
        cmd = f'set {_relation} [{cmd_str}]'
        result = Database.execute(db=db, cmd=cmd)
        if svar_name:  # Save the result using the supplied session variable name
            cls.set_var(db=db, name=svar_name)
        if result:
            return cls.make_pyrel(result)

    @classmethod
    def heading(cls, db: str, relation: str = _relation):
        cmd = f"relation heading ${{{relation}}}"
        result = Database.execute(db=db, cmd=cmd)
        return result

    @classmethod
    def semiminus(cls, db: str, rname2: str = _relation, attrs: Optional[Dict[str, str]] = None, rname1: str = _relation,
                 svar_name: Optional[str] = None) -> RelationValue:
        """
        Computes the difference between rname2 and the semijoin of rname1 and rname2.

        From TclRAL docs: The semiminus subcommand computes the difference between relationValue2 and the semijoin
        of relationValue1 and relationValue2. The returned relation has a heading equal to that of relationValue2
        and a body consisting of those tuples from relationValue2 which would not have been included in the natural
        join of relationValue1 and relationValue2.

        Args:
            db: DB session name.
            rname2: Name of the primary relation (result has the same heading as this).
            attrs: Optional attribute mapping dictionary for the join.
            rname1: Name of the filtering relation.
            svar_name: Relation result is stored in this optional TclRAL variable for subsequent operations to use.

        Returns:
            Resulting relation as a PyRAL relation value.
        """
        if attrs is None:
            attrs = {}
        cmd = f"set {{{_relation}}} [{cls._cmd_semiminus(rname1=rname1, rname2=rname2, attrs=attrs)}]"
        result = Database.execute(db=db, cmd=cmd)
        if svar_name:  # Save the result using the supplied session variable name
            cls.set_var(db=db, name=svar_name)
        return cls.make_pyrel(result)

    @classmethod
    def extend(cls, db: str, attrs: dict[str, str | int | float | bool], relation: str = _relation,
               svar_name: Optional[str] = None):
        """
        Returns a new relation with the same heading as the input extended by zero or more additional attributes.

        From TclRAL docs: The extend subcommand returns a new relation which has the same heading as relationValue
        with zero or more additional attributes. As each tuple in the body of relationValue is considered, its value
        is set into the variable whose name is given by the tupleVariable argument. This variable is accessible to
        the extending expressions so that the current tuple values are available for computing the new attribute values.

        Args:
            db: DB session name.
            attrs: Dictionary keyed by attribute name with values specifying the constant extension values.
            relation: Extend this relation.
            svar_name: Set this TclRAL relation variable to the result.

        Returns:
            Resulting relation as a PyRAL relation value.
        """
        cmd = f"set {_relation} [{cls._cmd_extend(relation=relation, attrs=attrs)}]"
        result = Database.execute(db=db, cmd=cmd)
        if svar_name:  # Save the result using the supplied session variable name
            cls.set_var(db=db, name=svar_name)
        return cls.make_pyrel(result)

    @classmethod
    def project(cls, db: str, attributes: Sequence[str], exclude: bool = False, relation: str = _relation,
                svar_name: Optional[str] = None) -> RelationValue:
        """
        Returns a relation whose heading consists of only a set of selected attributes.
        The body of the result consists of the corresponding tuples from the specified relation,
        removing any duplicates created by considering only a subset of the attributes.

        Args:
            db: DB session name.
            attributes: Attributes to be projected.
            exclude: If true, all attributes will be returned except for those in the attributes tuple.
            relation: The relation to be projected.
            svar_name: Relation result is stored in this optional TclRAL variable for subsequent operations to use.

        Returns:
            Resulting relation as a PyRAL relation value.
        """
        cmd = f"set {_relation} [{cls._cmd_project(db=db, relation=relation, attributes=attributes, exclude=exclude)}]"
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

        A convenience form of summarize where the per relation is a projection of the relation value being
        summarized. Rather than supplying a per relation, a list of attributes is given and the input relation
        is projected on those attributes and used as the per relation.

        Only one summarization operation is currently supported by PyRAL: count.

        From TclRAL man page: The summarizeby subcommand is a more convenient form of summarize where the per
        relation is a projection of the relation value that is to be summarized.

        TclRAL syntax::

            relation summarizeby relationValue attrList relationVarName attr type expression ?attr type expression ...?

        TclRAL command example::

            % relformat [relation summarizeby $OWNERSHIP Acquired s NumAcquired int {[relation cardinality $s]}]

        PyRAL example input::

            Relation.summarizeby(db=stdb, relation='Region', attrs=['Data_box', 'Title_block_pattern'],
                                 sum_attr=Attribute(name='Qty', type='int'), svar_name="Number_of_regions")

        Generates the TclRAL command::

            relation summarizeby ${Region} {Data_box Title_block_pattern} s Qty int {[relation cardinality $s]}

        Args:
            db: DB session name.
            relation: The relation to summarize.
            attrs: List of attribute names to summarize by.
            sum_attr: An Attribute named tuple defining the name and type of the summary attribute.
            op: The summarization operation (currently only 'count' is supported).
            svar_name: Relation result is stored in this optional TclRAL variable for subsequent operations to use.

        Returns:
            Resulting relation as a PyRAL relation value.
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
        Returns a new relation extended by a rank attribute.

        TclRAL documentation and syntax::

            relation rank relationValue ?-ascending | -descending? rankAttr newAttr

        The rank subcommand returns a new relation whose heading is the same as relationValue
        extended by an attribute named newAttr of type int, whose value is set to the number of tuples
        in relationValue where the value of rankAttr is less than or equal to (descending) or greater than
        or equal to (ascending) that of the given tuple. The default ranking is ascending.

        PyRAL example::

            result = Relation.rank(db=ev, relation="shafts_rv", sort_attr_name="Speed", order=Order.DESCENDING)

        Args:
            db: DB session name.
            order: Ascending or descending sort order.
            sort_attr_name: The values of this attribute will be sorted (TclRAL rankAttr).
            rank_attr_name: The name of the added rank number attribute (TclRAL newAttr).
            relation: Relation to be sorted.
            svar_name: An optional session variable that holds the result.

        Returns:
            Relation with the added rank attribute.
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
        Creates a new relation extended by an integer tag attribute with values between 0 and cardinality - 1.

        TclRAL documentation::

            relation tag relationValue attrName ?-ascending | -descending sort-attr-list? ?-within attr-list?

        Tuples in relationValue are extended in either ascending or descending order of the sort-attr-list.
        If no sort-attr-list is given, the tagging order is arbitrary.

        PyRAL implements this partially; the -within option is not yet supported.

        Args:
            db: DB session name.
            tag_attr_name: Name of the new integer tag attribute.
            sort_attrs: Tuple of attribute names to sort by before tagging.
            order: Ascending or descending sort order.
            relation: The relation to tag.
            svar_name: An optional session variable that holds the result.

        Returns:
            Relation with the added tag attribute.
        """
        do_sort = "" if not sort_attrs else f" -{order.value} {' '.join(sort_attrs)} "
        cmd = f"set {_relation} [relation tag ${{{snake(relation)}}} {{{tag_attr_name}}} {do_sort}]"
        result = Database.execute(db=db, cmd=cmd)
        if svar_name:
            cls.set_var(db=db, name=svar_name)
        return cls.make_pyrel(result)

    @classmethod
    def rank_restrict(cls, db: str, attr_name: str, extent: Extent, card: Card, relation: str = _relation,
                      svar_name: Optional[str] = None) -> RelationValue:
        """
        Order the tuples of a relation on some attribute and select those at the furthest extent.

        For example, to select the highest flying aircraft: rank all aircraft tuples by altitude in
        descending order, then select all those of ranking 1. Since multiple aircraft might be flying
        at the same highest altitude, there may be multiple ranked as 1.

        If the user specifies the ALL cardinality, multiple tuples at the same extent may be returned.
        The ONE cardinality specifies that only one will be selected and the user cannot choose which.

        Args:
            db: DB session name.
            attr_name: Tuples are ordered based on values of this attribute.
            extent: Greatest or least extent to select.
            card: One or all tuples at that extent.
            relation: Selection is on tuples of this relation.
            svar_name: Relation result is stored in this optional TclRAL variable for subsequent operations to use.

        Returns:
            The tuple or tuples at the specified extent.
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
        Select zero or more tuples that match the supplied criteria (relational restriction).

        TclRAL syntax::

            relation restrictwith <relationValue> <expression>

        The most common usage scenario is to select on a single identifier attribute value::

            R = f"ID:S1"
            result = Relation.restrict(db=ev, relation="shafts_rv", restriction=R, svar_name="restriction")

        This yields the TclRAL statement::

            relation restrictwith ${shafts_rv} {[string match {S1} $ID]}

        If there is any whitespace in the value, use <> brackets to enclose it::

            R = f"Level_name:<3rd Floor>"

        Note that attribute names must be in snake case. The : symbol is shorthand for string matching only.
        The == operator can only be used for numeric values.

        You can AND multiple conditions using ', '::

            PyRAL: R = f"ID:<{i}>, In_service:<{True}>"
            TclRAL: {[string match {S1} $ID] && [string match {True} $In_service]}

        Numeric comparisons use standard operators::

            PyRAL: R = f"Speed > {s}"
            TclRAL: {[expr {$Speed > 14}]}

        Complex logic with nested parentheses and AND, OR, NOT::

            PyRAL: R = f"ID:<{v}> OR (In_service:<{True}> AND Speed > {s})"
            TclRAL: {[string match {S1} $ID] || ([string match {True} $In_service] && [expr {$Speed > 31}])}

        Args:
            db: DB session name.
            restriction: A string in Scrall notation that specifies the restriction criteria.
            relation: Name of a relation variable where the operation is applied.
            svar_name: An optional session variable that holds the result.

        Returns:
            The TclRAL string result representing the restricted tuple set.
        """
        cmd = f"set {_relation} [{cls._cmd_restrict(relation=relation, restriction=restriction)}]"
        result = Database.execute(db=db, cmd=cmd)
        if svar_name:
            cls.set_var(db=db, name=svar_name)
        return cls.make_pyrel(result)
