#!/bin/tcsh
# Set neccessary environement variables
source /sgpvlbi/progs/set_csolve_test.csh
#
# Set environment for ADAP application
setenv APP_DIR /sgpvlbi/progs/adap_v2
setenv CONFIG_DIR /sgpvlbi/progs/config/adap
setenv PROBLEM_DIR /sgpvlbi/progs/logs/adap/problems
setenv PYTHONPATH /sgpvlbi/progs/adap_v2/src
#
source $APP_DIR/venv/bin/activate.csh

