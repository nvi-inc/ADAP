# Set neccessary environement variables
setenv MK5_ROOT "/sgpvlbi/progs/csolve_2021-04-20"
setenv OMP_NUM_THREADS 12
set prompt="[$1 %n@%m:%c >"
# 
#
setenv VGOSDB_DIR  /sgpvlbi/level2/vgosDB
setenv SAVE_DIR    /sgpvlbi/apriori/save_dir
setenv MODEL_DIR   /sgpvlbi/apriori/models
setenv MK5_APRIORI /sgpvlbi/apriori/solve
setenv WORK_DIR    /sgpvlbi/scratch/work_dir
setenv SPOOL_DIR   /sgpvlbi/scratch/spool_dir
setenv SHARE       /sgpvlbi/group/share
#setenv SUPCAT_FILE /500/oper/solve_save_files/super.cat.calc11
#setenv SUPCAT_DIR_LIST /500/oper/solve_save_files/super.dir.calc11
# Set compiler variables
# adjust path
setenv PATH ${PATH}:/sgpvlbi/progs/scripts:$MK5_ROOT/bin:/sgpvlbi/space/auxSoft/bin
#Sergei says this is dangerous. Why?
setenv PATH "./:$PATH"

setenv MK5_FC /opt/intel/oneapi/compiler/2022.0.2/linux/bin/intel64/ifort
#
#
# we need this to set up paths to ICC's shared libraries:
#
# do not have to be here (proper init is in .bashrc):
#   source /opt/intel/bin/compilervars.csh ia32
#
# aux shared libraries:
#
setenv LD_LIBRARY_PATH /sgpvlbi/progs/auxSoft/lib
#

