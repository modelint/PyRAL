# This software is copyrighted 2006 - 2014 by G. Andrew Mangogna.
# The following terms apply to all files associated with the software unless
# explicitly disclaimed in individual files.
# 
# The authors hereby grant permission to use, copy, modify, distribute,
# and license this software and its documentation for any purpose, provided
# that existing copyright notices are retained in all copies and that this
# notice is included verbatim in any distributions. No written agreement,
# license, or royalty fee is required for any of the authorized uses.
# Modifications to this software may be copyrighted by their authors and
# need not follow the licensing terms described here, provided that the
# new terms are clearly indicated on the first page of each file where
# they apply.
# 
# IN NO EVENT SHALL THE AUTHORS OR DISTRIBUTORS BE LIABLE TO ANY PARTY FOR
# DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES ARISING
# OUT OF THE USE OF THIS SOFTWARE, ITS DOCUMENTATION, OR ANY DERIVATIVES
# THEREOF, EVEN IF THE AUTHORS HAVE BEEN ADVISED OF THE POSSIBILITY OF
# SUCH DAMAGE.
# 
# THE AUTHORS AND DISTRIBUTORS SPECIFICALLY DISCLAIM ANY WARRANTIES,
# INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE, AND NON-INFRINGEMENT.  THIS SOFTWARE
# IS PROVIDED ON AN "AS IS" BASIS, AND THE AUTHORS AND DISTRIBUTORS HAVE
# NO OBLIGATION TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS,
# OR MODIFICATIONS.
# 
# GOVERNMENT USE: If you are acquiring this software on behalf of the
# U.S. government, the Government shall have only "Restricted Rights"
# in the software and related documentation as defined in the Federal
# Acquisition Regulations (FARs) in Clause 52.227.19 (c) (2).  If you
# are acquiring the software on behalf of the Department of Defense,
# the software shall be classified as "Commercial Computer Software"
# and the Government shall have only "Restricted Rights" as defined in
# Clause 252.227-7013 (c) (1) of DFARs.  Notwithstanding the foregoing,
# the authors grant the U.S. Government and others acting in its behalf
# permission to use and distribute the software in accordance with the
# terms specified in this license.
# 
#  *++
# MODULE:
# ralutil.tcl -- Convenience scripts for use with TclRAL
# 
# ABSTRACT:
# This file contains a set of utility procs that are useful in
# conjunction with TclRAL. N.B. that many of these are experimental
# in nature and interfaces are subject to much change. Part of the
# purpose for this package is to determine what works well in practice
# without cluttering the TclRAL package proper.
# 
#  *--

package provide ralutil 0.12.2

package require ral 0.12.2

namespace eval ::ralutil {
    namespace export pipe
    namespace export sysIdsInit
    namespace export sysIdsGenSystemId
    namespace export crosstab
    namespace export rvajoin
    namespace export attrConstraint
    namespace ensemble create

    namespace import ::ral::*

    # We define the very special relation values of DEE and DUM.
    # DUM is the relation value that has an empty heading and an empty body.
    # DEE is the relation value that has an empty heading and a body consisting
    # of one tuple that is the empty tuple (the only body that an empty heading
    # relation value could have).
    variable DUM {{} {}}
    variable DEE {{} {{}}}
}

# Relational expressions are frequently nested to achieve some desired
# result. This nesting in normal Tcl syntax quickly gets difficult to read.
# Often the expression is naturally a pipe of the output of one command
# to the input of another. This idea came from the "Commands pipe" page
# on the wiki: http://wiki.tcl.tk/17419. The code here is different,
# but the idea is the same.
proc ::ralutil::pipe {fns {var {}} {sep |~}} {
    # Split out the separator characters
    lassign [split $sep {}] s p
    # Split up the commands based on the separator character, pulling off the
    # first command.
    set pipeline [lassign [split $fns $s] cmd]
    # Trim off whitespace so that the input can be more free form.
    set cmd [string trim $cmd]
    # Iterate over the remaining elements in the pipeline
    foreach elem $pipeline {
        set elem [string trim $elem]
        # If there is no placeholder character in the command, then the
        # pipeline result is just placed as the last argument. Otherwise, the
        # accumulated pipeline is substituted for the placeholder.  N.B. the
        # use of "string map" implies that _all_ the placeholders will be
        # replaced.
        set cmd [expr {[string first $p $elem] == -1 ?\
            "$elem \[$cmd\]" : [string map [list $p "\[$cmd\]"] $elem]}]
    }
    # perform the command or save it into a variable
    if {$var eq {}} {
	return [uplevel 1 $cmd]
    } else {
	upvar 1 $var v
	set v $cmd
    }
}

# These procs form a scheme for system defined identifiers.
# This scheme works as a reasonable way to assign system identifiers
# that are based on incrementing integers.
# Note that is possible to save and restore the contents of the
# relvar holding the identifiers, although that is not done here.
proc ::ralutil::sysIdsInit {} {
    if {![relvar exists ::__ral_systemids__]} {
	relvar create ::__ral_systemids__\
            {RelvarName string IdAttr string IdNum int} {RelvarName IdAttr}
    }
}

# A convenience proc that installs a trace on an attribute of
# a relvar in order to have the system assign a unique value
# to that attribute.
proc ::ralutil::sysIdsGenSystemId {relvarName attrName} {
    relvar trace add variable [uplevel 1 ::ral::relvar path $relvarName] insert\
	[list ::ralutil::sysIdsCreateIdFor $attrName]
}

# This proc can be used in a relvar trace on insert to
# create a unique identifier for an attribute.
proc ::ralutil::sysIdsCreateIdFor {attrName op relvarName tup} {
    relvar eval {
	set updated [\
	    relvar updateone __ral_systemids__ sysid\
		[list RelvarName $relvarName IdAttr $attrName] {
		set idValue [tuple extract $sysid IdNum]
		tuple update $sysid IdNum [incr idValue]
	    }]
	if {[relation isempty $updated]} {
	    # If we don't update then we need to 
	    # find the maximum value of the atttribute in any tuple
	    # to use as our counter value.
	    set idValue [findMaxAttrValue $relvarName $attrName]
	    # The next one to use is one past the max
	    relvar insert __ral_systemids__ [list RelvarName $relvarName\
		IdAttr $attrName IdNum [incr idValue]]
	}
    }
    # It is possible that no value was given for the attribute that is to be
    # generated. In that case we need to add it in using the type we find in
    # the relation heading.
    if {$attrName in [tuple attributes $tup]} {
        set newTup [tuple update $tup $attrName $idValue]
    } else {
        set heading [relation heading [relvar set $relvarName]]
        set attrType [dict get $heading $attrName]
        set newTup [tuple extend $tup $attrName $attrType $idValue]
    }
    return $newTup
}

# A helper procedure in finding the value for a system identifiers
proc ::ralutil::findMaxAttrValue {relvarName attrName} {
    set relValue [relvar set $relvarName]
    # If we don't find any, then we just start at 0
    set idValue 0
    if {[relation isnotempty $relValue]} {
	# We find the maximum by summarizing over DEE
	set idValue [pipe {
	    relation summarizeby $relValue {} r\
		maxValue int {rmax($r, $attrName)} |
	    relation extract ~ maxValue
        }]
    }
    return $idValue
}

# crosstab relValue crossAttr ?attr1 attr2 ...?
#
# Generate a cross tabulation of "relValue" for the "crossAttr" against the
# variable number of attributes given. The "crossAttr" argument is the name of
# an attribute of "relValue". The idea is to create new relation that contains
# all the attributes in "args" plus a new attribute for each distinct value of
# "crossAttr". The value of the new attributes is the count of tuples that have
# the corresponding value of "crossAttr".  Relationally, the "summarize"
# command is used when computations are required across groups of tuples.
proc ::ralutil::crosstab {relValue crossAttr args} {
    # We start by projecting the attributes that will be retained
    # in the resulting relation.
    set subproj [relation project $relValue {*}$args]
    # The strategy is to build up a summarize command on the fly, adding new
    # attributes. So we start with the constant part of the command.
    set sumCmd [list relation summarize $relValue $subproj r]

    # By projecting on the "crossAttr" we get the unique set of values
    # for that attribute since there are no duplicates in relations.
    set crossproj [relation project $relValue $crossAttr]
    # For each distinct value of the "crossAttr" extend the relation with
    # a new attribute by the same name as the value and whose value is
    # the number of tuples which match the value.
    foreach val [lsort [relation list $crossproj]] {
	 set sumexpr [format\
	     {[relation cardinality [relation restrictwith $r {$%s == "%s"}]]}\
	     $crossAttr $val]
	 lappend sumCmd $val int $sumexpr
    }
    # Finally we want the total for all the "crossAttr" matches.
    lappend sumCmd Total int {[relation cardinality $r]}
    return [eval $sumCmd]
}

# Perform the relational equivalent to an "outer join". The idea is to end up
# with a relation that contains a relation valued attribute that contains the
# tuples that match in the join and that is empty for those tuples in the
# relation that do not match across the join attributes (the "natural
# join" leaving such tuples out of the result altogether).
# "r1" the first relation to join
# "r2" the second relation to join
# "newAttr" the name of the new relation valued attribute
# The remaining optional arguments are the attributes across which the join is
# to be made.  If the list is empty, then the commonly named attributes are
# found.
# The result has the same heading as "r1" plus an attribute "newAttr".
# The type of "newAttr" is Relation with the heading of "r2" minus
# the join attributes.

proc ::ralutil::rvajoin {r1 r2 newAttr args} {
    # If the join attributes are not given, then determine them by finding the
    # intersection of the attributes in r1 and r2.
    set joinAttrs $args
    if {[llength $joinAttrs] == 0} {
        set a1 [relation attributes $r1]
        set a2 [relation attributes $r2]
        foreach attr $a1 {
            if {$attr in $a2} {
                lappend joinAttrs $attr
            }
        }
    }
    # N.B. if there are no commonly named attributes, then we get the usual
    # degenerate behavior of a join, namely, every tuple in the result relation
    # will have its relation valued attribute set to "r2" in a sort of
    # interesting take on the Cartesian product (I think if the result is
    # "ungroup"ed it is the Cartesian product.)

    # Compute the heading of the relation valued attribute by removing the
    # join attributes from the heading of "r2".
    set rvaHeading [relation heading $r2]
    foreach attr $joinAttrs {
        set attrIndex [lsearch -exact $rvaHeading $attr]
        if {$attrIndex == -1} {
            error "attribute, \"$attr\", does not appear in,\
                    \"$[relation attributes $r2]\""
        } else {
            set rvaHeading [lreplace $rvaHeading $attrIndex $attrIndex+1]
        }
    }
    # Finally, compute the result by extending "r1" with the relation valued
    # attribute. The tuples of the relation valued attribute are those where
    # the corresponding attributes in "r1" match those in "r2".
    return [relation extend $r1 r1tup $newAttr [list Relation $rvaHeading] {
        [relation project [relation restrict $r2 r2tup {
            [tuple equal [tuple project $r2tup {*}$joinAttrs]\
                    [tuple project $r1tup {*}$joinAttrs]]
        }] {*}[dict keys $rvaHeading]]
    }]
}

# This procedure provides a simpler interface to relvar tracing.
# It adds a variable trace for insert and update
# on the given "relvarName". The trace
# evaluates "expr" and if true, allows the insert or update to procede.
# The expression may contain references to attribute names of the form
# :<attr name>: (e.g. for an attribute called A1, "expr" may contain
# tokens of :A1:). When the trace is evaluated, all of the attribute
# tokens are replaced by the value of the attribute.
# The intended use case is when you wish to constrain an attribute value
# to be within some subrange of the data type. For example, if an int
# typed attribute called, "Status" should be > 0 and < 22, then
#
#       attrConstraint myRelvar {:Status: > 0 && :Status: < 22}
#
# will install a variable trace on "myRelvar" to insure that each
# insert or update to it has a proper value for "Status".
proc ::ralutil::attrConstraint {relvarName expr} {
    set attrs [relation attributes [uplevel 1 ::ral::relvar set $relvarName]]
    foreach attr $attrs {
        set expr [regsub -all -- ":${attr}:" $expr\
                " \[::ral::tuple extract \$newTup $attr\] "]
    }
    set body [format {
        if {$op eq  "insert"} {
            set newTup [lindex $args 0]
        } elseif {$op eq "update"} {
            set newTup [lindex $args 1]
        } else {
            error "unknown relvar operation, \"$op\""
        }
        if {%s} {
            return $newTup
        } else {
            error "constraint, \"{%s}\", on $relvarName failed"
        }
    } $expr $expr]
    uplevel 1 [list ::ral::relvar trace add variable $relvarName\
            {insert update} [list ::apply [list {op relvarName args} $body]]]
}
