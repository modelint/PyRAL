# You need to download the tcl ral and ralutil packages for your platform
# See README.md for a link to the tclral site
# Then you must edit the line below so that it is the path to where those packages reside on your platform
set RPATH /Users/starr/SDEV/TclRAL

::tcl::tm::path add $RPATH

package require ral
package require ralutil

namespace import ral::*
namespace import ralutil::*