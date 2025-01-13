#!/bin/tcsh
#
unsetenv DISPLAY

setenv SUDO_USER "oper"

source /sgpvlbi/progs/adap_v2/scripts/setup.csh

# Set XDG tempopary folder for sudo_user
setenv XDG_RUNTIME_DIR  /tmp/runtime-$SUDO_USER
#
setenv NUSOLVE_AUTO "ADAP"

nohup python $APP_DIR/src/VLBIvgosdb.py -c $ADAP_CONFIG -q VLBIvgosdb >& $PROBLEM_DIR/"vgosdb.txt" &
