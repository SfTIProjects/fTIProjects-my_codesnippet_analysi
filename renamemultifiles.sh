#!/bin/bash

# Usage
# $(basename $0) <directory> <pattern>"
# $(basename $0) my/files 1000 snips%s.zip"

FILE_LIST=( $(find $1 -maxdepth 1 -type f -name "*.csv") )
NUM_FILES="$(echo "${FILE_LIST[*]}" | wc -w)"

for ((file = 0; file < NUM_FILES; file ++));do
    mv -i "${file}" "${PMDJavaCodeSnippetsVionations}"
    FILES=( "${FILE_LIST[@]:$i:$2}" )
    RANGE="$(printf "%0${#NUM_FILES}d-%0${#NUM_FILES}d" $i $((i + ${#FILES[@]})))"
    ZIP_NAME="$1/$(printf "$3" $RANGE)"
    eval "zip -0 -qmj $ZIP_NAME ${FILES[*]} && echo $ZIP_NAME created." & 
done

wait

echo "Finished!!"