import time
from datetime import timedelta

import html
import re

import numpy as np
import pandas as pd
import xml.etree.ElementTree as et

import dask
import dask.dataframe as dd
import dask.bag as bd
from dask.distributed import Client
from dask_jobqueue import SLURMCluster


##########################################################
#Step 1. Format 'pmd_rules_results.xml' from Linux terminal#
##########################################################
# -'pmd_rules_results.xml' is not properly formated
# -We used linux format command to format 'pmd_rules_results.xml' to save it in 'pmd_rules_results_fmt.xml'
# $  xmllint --format pmd_rules_results.xml > pmd_rules_results_fmt.xml
##############################################################################
#Step2. Remove some lines in 'test_flaws_rem_sm_tags_fmt' from Linux terminal#
##############################################################################
# remove lines 1 to 2 and the last line of the 'pmd_rules_results_fmt.xml'
# MacOS $  sed -i '' '1,2d;$d' pmd_rules_results_fmt.xml 
# Linux  $  sed -i '1,2d;$d' pmd_rules_results_fmt.xml 
# blocksize breaks it into partitions

pmd_rules_result_bd = bd.read_text('pmd_rules_results_fmt.xml', blocksize='10MB', linedelimiter='</violation>\n')


# check to see number of partitions
# find the number of partitions
#pmd_rules_result_bg.npartitions # or
pmd_rules_result_bd



### We are intrested in getting the 'violation'
#- so we can get rid of the files
#- then filter

# replace all the '<file>' with '</file>' so that we can access it using xml.etree.ElementTree
violations_rows_bd = pmd_rules_result_bd.map(lambda line: re.sub(r'\s*<\s*file.*>|.*/file>', '', line))

# look at the first row
violations_rows_bd.take(1)

#filter only <violation ...
# Regex can also be applied here
#violations_rows_top_bd = violations_rows_bd.filter(lambda line: re.findall(r'\s*<violation', line))
violations_rows_bd = violations_rows_bd.filter(lambda line: line.find('<violation' or '< violation') >= 0)

# look at the first row
violations_rows_bd.take(1)


### Use ElementTree to get all the attrbutes and text in xmlor html tags
#- To get both the the attrbutes and text in the violation tag 
#    - We use add_text_to_dict_attrb() to achieve that

def add_text_to_dict_attrb(dictionary, txt):
    dictionary['text'] = txt
    return dictionary

# This will give us both the attributes in the violation tag and the text enclosed in the violation tag:
# <violation ...> ... </violation>
violations_rows_bd = violations_rows_bd.map(lambda row:  add_text_to_dict_attrb(et.fromstring(row).attrib, et.fromstring(row).text))

# This will give us both the attributes in the violation tag and the text enclosed in the violation tag:
# <violation ...> ... </violation>
violations_rows_bd = violations_rows_bd.map(lambda row:  add_text_to_dict_attrb(et.fromstring(row).attrib, et.fromstring(row).text))


# look at the first row
print('Take a look at the first row {}'.format(violations_rows_bd.take(1)))

# Convert Bags to Dataframes
df = violations_rows_bd.to_dataframe()

# View Stucture
print('View Structure {}'.format(df))

print('View Columns {}'.format(df.columns))

## Start a Dask cluster using SLURM jobs as workers.
#http://jobqueue.dask.org/en/latest/generated/dask_jobqueue.SLURMCluster.html
dask.config.set(
    {
        "distributed.worker.memory.target": False,  # avoid spilling to disk
        "distributed.worker.memory.spill": False,  # avoid spilling to disk
    }
)
cluster = SLURMCluster(
    cores=10, #cores=24, # we set each job to have 1 Worker, each using 10 cores (threads) and 8 GB of memory
    processes=2,
    memory="32GiB",
    walltime="0-00:30",# walltime="0-00:50",
    log_directory="../dask/logs",  # folder for SLURM logs for each worker
    local_directory="../dask",  # folder for workers data
)

#cluster.adapt(minimum_jobs=20, maximum_jobs=200)
cluster.adapt(minimum_jobs=5, maximum_jobs=10)
client = Client(cluster)
client

print(df.head().compute())
print('Length {}'.format(df.shape[0].compute()))

#replace new lines with empty strings
df = df.replace('\\n','', regex=True)

print(df.head().compute())
print('Length after removing newline characters {}'.format(df.shape[0].compute()))

# drop empty columns
df = df[~df['class'].isna()] # Drop rows that have NaN in the Code column
print('Length after removing NaNs {}'.format(df.shape[0].compute()))

# drop classes that do not have this pattern
df = df[df['class'].str.contains('^Code_\d+_\d+_\d+_\d+', regex=True)]
print('Length after droping errors due introduced by programmer {}'.format(df.shape[0].compute()))

# select multiple columns
#df = df[['beginline', 'endline', 'begincolumn', 'endcolumn', 'rule', 'ruleset','class', 'externalInfoUrl', 'priority', 'text']]
#df = df[['class', 'rule', 'ruleset', 'text']]

#Save to a csv file

## Save all the codes from the posts into a CSV file

# Save to a CSV file
_=df.to_csv('pmdcodesnippets_csv2/PMDJavaCodeSnippets*.csv', sep=',', index=False)