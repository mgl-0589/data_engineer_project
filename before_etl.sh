#!/bin/bash

set -o allexport
source ./.env
set +o allexport


YEAR=$(date +%Y%m%d | cut -c1-4)
LOCAL_DIRECTORY=$LOCAL_DIRECTORY
WSL_DIRECTORY=$HOME_DIRECTORY

echo $LOCAL_DIRECTORY
echo $HOME_DIRECTORY

count_invoices=$(ls -l ${LOCAL_DIRECTORY}/{*.xml,*.pdf,*.zip} | wc -l)
count_sales=$(ls -l ${LOCAL_DIRECTORY}/*.xls | wc -l)


if [ $count_invoices -gt 0 ]; then
    echo "moving $count_invoices invoice files ..."
    
    # moving .xml, .pdf, and .zip files into source_files/invoices directory
    cp ${LOCAL_DIRECTORY}/{*.xml,*.pdf,*.zip} ${HOME_DIRECTORY}/source_files/invoices

fi

if [ $count_sales -gt 0 ]; then
    echo "moving $count_sales sale files ..."

    # moving .xls files into source_files/sales directory
    cp ${LOCAL_DIRECTORY}/*.xls ${HOME_DIRECTORY}/source_files/sales
fi