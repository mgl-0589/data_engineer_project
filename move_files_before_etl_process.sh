#!/bin/bash

YEAR=$(date +%Y%m%d | cut -c1-4)
LOCAL_DIRECTORY="/mnt/c/Users/ingmg/Documents"
WSL_DIRECTORY="/home/mgl/repos/coffee_shop_data_project"

count_invoices=$(ls -l ${LOCAL_DIRECTORY}/{*.xml,*.pdf,*.zip} | wc -l)
count_sales=$(ls -l ${LOCAL_DIRECTORY}/*.xls | wc -l)


if [ $count_invoices -gt 0 ]; then
    echo "moving $count_invoices invoice files ..."
    
    # moving .xml, .pdf, and .zip files into source_files/invoices directory
    cp ${LOCAL_DIRECTORY}/{*.xml,*.pdf,*.zip} ${WSL_DIRECTORY}/source_files/invoices

fi

if [ $count_sales -gt 0 ]; then
    echo "moving $count_sales sale files ..."

    # moving .xls files into source_files/sales directory
    cp ${LOCAL_DIRECTORY}/*.xls ${WSL_DIRECTORY}/source_files/sales
fi

