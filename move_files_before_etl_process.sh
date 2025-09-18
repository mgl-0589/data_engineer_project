#!/bin/bash

YEAR=$(date +%Y%m%d | cut -c1-4)

count_invoices=$(ls -l ${LOCAL_DIRECTORY}/{*.xml,*.pdf,*.zip} | wc -l)
count_sales=$(ls -l ${LOCAL_DIRECTORY}/*.xls | wc -l)


if [ $count_invoices -gt 0 ]; then
    echo "moving $count_invoices invoice files ..."
    
    # moving .xml, .pdf, and .zip files into source_files/invoices directory
    cp ${LOCAL_DIRECTORY}/{*.xml,*.pdf,*.zip} /home/mgl/repos/coffee_shop_data_project/source_files/invoices

fi

if [ $count_sales -gt 0 ]; then
    echo "moving $count_sales sale files ..."

    # moving .xls files into source_files/sales directory
    cp ${LOCAL_DIRECTORY}/*.xls /home/mgl/repos/coffee_shop_data_project/source_files/sales
fi

echo $YEAR