#!/bin/bash

set -o allexport
source ./.env
set +o allexport


YEAR=$(date +%Y%m%d | cut -c1-4)
date=$(date "+%Y%m%d")

LOCAL_DIRECTORY=$LOCAL_DIRECTORY
WSL_DIRECTORY=$HOME_DIRECTORY


for ext in xml pdf xlsx; do

    count=$(ls -l $LOCAL_DIRECTORY/*.$ext | wc -l)
    echo ""
    echo "$count $ext file(s)"
    echo ""

    if [ $count -gt 0 ]; then
        echo "compressing $count file(s) ..."

        cd ${LOCAL_DIRECTORY}/
        echo $(pwd)
        file=archive_${ext}_${date}.tar.gz

        # validating if .tar.gz file already exists
        if [ -s $file ]; then
            
            echo "file already exists ... "
            # extract files from .tar.gz file
            cd ${LOCAL_DIRECTORY}/
            # tar -xzvf archive_${ext}_${date}.tar.gz

            # remove .tar.gz empty file
            # rm -rf $LOCAL_DIRECTORY/$file

        fi

        # compress all files
        cd ${LOCAL_DIRECTORY}/
        # tar -czvf archive_${ext}_${date}.tar.gz *.$ext

        # remove files
        # rm -rf $LOCAL_DIRECTORY/{*.xml,*.pdf,*.xlsx}

    fi
done
