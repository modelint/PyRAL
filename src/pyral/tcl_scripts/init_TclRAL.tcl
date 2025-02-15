# You need to download the tcl ral and ralutil packages for your platform
# See README.md for a link to the tclral site
# Then you must edit the line below so that it is the path to where those packages reside on your platform
#set RPATH /Users/starr/SDEV/TclRAL
# Get the directory where the script is located

set BASE_DIR [file dirname [info script]]

# Determine system architecture
set arch [exec uname -m]

# Set the appropriate subdirectory based on architecture
if {$arch eq "arm64"} {
    set RPATH [file join $BASE_DIR "MacSilicon"]
} else {
    set RPATH [file join $BASE_DIR "MacIntel"]
}

# Debugging output
#puts "RPATH set to: $RPATH"

::tcl::tm::path add $RPATH

package require ral
package require ralutil

namespace import ral::*
namespace import ralutil::*