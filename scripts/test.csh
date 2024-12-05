#!/usr/bin/csh
# Set neccessary environement variables
source /sgpvlbi/progs/set_csolve.csh
#
# Set environment for ADAP application
setenv ADAP_DIR /sgpvlbi/progs/adap_v2
setenv CONFIG_DIR /sgpvlbi/progs/adap_v2/config
setenv PROBLEM_DIR /sgpvlbi/progs/logs/adap/problems
setenv PYTHONPATH /sgpvlbi/progs/adap_v2/src
#
# Set XDG tempopary folder for sudo_user
setenv XDG_RUNTIME_DIR  /tmp/runtime-$SUDO_USER
#
# Set QT environment variables
setenv QT_LOGGING_RULES '*.debug=false;qt.qpa.*=false'

# Start virtual enviroment
source /sgpvlbi/progs/adap/venv/bin/activate.csh

python /sgpvlbi/progs/adap/src/qaps $argv 
#>& /dev/null 

