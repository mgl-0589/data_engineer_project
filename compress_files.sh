#!/bin/bash

set -o allexport
source ./.env
set +o allexport


YEAR=$(date +%Y%m%d | cut -c1-4)
date=$(date "+%Y%m%d")

LOCAL_DIRECTORY=$LOCAL_DIRECTORY
WSL_DIRECTORY=$HOME_DIRECTORY
TMP_DIRECTORY=$TMP_DIRECTORY


for ext in xml pdf xlsx; do

    count=$(ls -l ${WSL_DIRECTORY}/${TMP_DIRECTORY}/*.$ext | wc -l)
    echo ""
    echo "$count $ext file(s)"
    echo ""

    if [ $count -gt 0 ]; then
        echo "compressing $count file(s) ..."

        cd $LOCAL_DIRECTORY/
        file=archive_${ext}_${date}.tar.gz

        # validating if .tar.gz file already exists
        if [ -s $file ]; then

            echo "file already exists ... "

            # move old .tar.gz file to tmp folder
            mv $LOCAL_DIRECTORY/*.tar.gz ${WSL_DIRECTORY}/${TMP_DIRECTORY}/

            # extract files from .tar.gz file
            cd ${WSL_DIRECTORY}/${TMP_DIRECTORY}/
            tar -xzvf archive_${ext}_${date}.tar.gz

            # remove old .tar.gz file
            cd ${WSL_DIRECTORY}/${TMP_DIRECTORY}/
            # rm -rf ${WSL_DIRECTORY}/${TMP_DIRECTORY}/*.tar.gz

        fi

        # compress all files
        cd ${WSL_DIRECTORY}/${TMP_DIRECTORY}/
        tar -czvf archive_${ext}_${date}.tar.gz *.$ext

        # move files
        mv ${WSL_DIRECTORY}/${TMP_DIRECTORY}/*.tar.gz $LOCAL_DIRECTORY/

        # remove files from tmp folder
        rm -rf ${WSL_DIRECTORY}/${TMP_DIRECTORY}/*.$ext

    fi
done
