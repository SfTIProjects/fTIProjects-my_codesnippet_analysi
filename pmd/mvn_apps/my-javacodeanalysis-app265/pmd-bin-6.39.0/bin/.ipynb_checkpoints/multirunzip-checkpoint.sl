#!/bin/bash -e

#SBATCH --time 0:30:00
#SBATCH --mem 1G
#SBATCH --output slurmlog.txt  # output of the script that runs the code analysis use slurmlog%a.txt in saparate files
#SBATCH --open-mode append
#SBATCH --array 0-19

# Operating on zipped files about 15% faster.

PATH_BASE="/nesi/nobackup/uoo03396/SfTI_project_nobackup/SfTI_Projects/StackOverflow_project/my_codesnippet_analysis"
PATH_BIN_DIR="$PATH_BASE/PMD/pmd-bin-6.39.0/bin"
PATH_RULES="$PATH_BASE/PMD/pmd_rules/pmdrules.xml"
PATH_INPUTS="$PATH_BASE/codesnippets_zip"
PATH_OUTPUT="$PATH_BASE/PMD/pmd_rules/pmd_rules_output_${SLURM_ARRAY_TASK_ID}.xml"

# Get list of all inputs. 
INPUTS_ALL=(${PATH_INPUTS}/*.zip)
# Get n'th tarball.
INPUT=${INPUTS_ALL[$SLURM_ARRAY_TASK_ID]}

echo "Using '${INPUT}'."
cd $PATH_BIN_DIR
./run.sh pmd -d $INPUT -f xml -R $PATH_RULES > $PATH_OUTPUT