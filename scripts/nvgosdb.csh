#!/usr/bin/tcsh
#
unsetenv DISPLAY

source /sgpvlbi/progs/adap_v2/scripts/setup-nvi.csh
# Set XDG tempopary folder for sudo_user
setenv XDG_RUNTIME_DIR  /tmp/runtime-$SUDO_USER
#
setenv NUSOLVE_AUTO "ADAP"

nohup python $APP_DIR/src/VLBIvgosdb.py -c $CONFIG_DIR/vlbi-nvi.toml -q VLBIvgosdb >& $PROBLEM_DIR/"vgosdb-nvi.txt" &
