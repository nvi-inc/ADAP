#!/bin/tcsh
#
# Set neccessary environement variables
source /sgpvlbi/progs/set_csolve.csh
#
# Set environment for ADAP application
#
set nvi = "nvi-vlbi-2"

setenv APP_DIR /sgpvlbi/progs/adap_v2
# Check if development variable is set
if ($?DEV_ENV) then
   if ( -d "$DEV_ENV") then
      setenv APP_DIR $DEV_ENV
   endif
endif

if ($HOST == $nvi) then
  setenv CONFIG_DIR /sgpvlbi/progs/config/adap_nvi
  setenv PROBLEM_DIR /sgpvlbi/progs/logs/adap_nvi/problems
else
  setenv CONFIG_DIR /sgpvlbi/progs/config/adap
  setenv PROBLEM_DIR /sgpvlbi/progs/logs/adap/problems
endif
#
setenv PYTHONPATH /sgpvlbi/progs/adap_v2/src
setenv ADAP_CONFIG $CONFIG_DIR/vlbi2023a.toml
#
source $APP_DIR/venv/bin/activate.csh

