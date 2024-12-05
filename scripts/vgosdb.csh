#!/bin/tcsh
#
unsetenv DISPLAY

setenv SUDO_USER "oper"

source /sgpvlbi/progs/adap_v2/scripts/setup.csh

# Set XDG tempopary folder for sudo_user
setenv XDG_RUNTIME_DIR  /tmp/runtime-$SUDO_USER
#
setenv NUSOLVE_AUTO "ADAP"

#nohup python $APP_DIR/src/VLBIvgosdb.py -c $CONFIG_DIR/vlbi2023.toml -q VLBIvgosdb >& $PROBLEM_DIR/"vgosdb.txt" &

nohup python $APP_DIR/src/VLBIvgosdb.py -c $CONFIG_DIR/vlbi2023a.toml -q VLBIvgosdb >& $PROBLEM_DIR/"vgosdb.txt" &
