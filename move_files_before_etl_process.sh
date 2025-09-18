#!/bin/bash

YEAR=$(date +%Y%m%d | cut -c1-4)

count_invoices=$(ls -l /mnt/c/Users/ingmg/Documents/{*.xml,*.pdf,*.zip} | wc -l)
count_sales=$(ls -l /mnt/c/Users/ingmg/Documents/*.xls | wc -l)


if [ $count_invoices -gt 0 ]; then
    echo "moving $count_invoices invoice files ..."
    
    # moving .xml, .pdf, and .zip files into source_files/invoices directory
    cp /mnt/c/Users/ingmg/Documents/{*.xml,*.pdf,*.zip} /home/mgl/repos/coffee_shop_data_project/source_files/invoices

fi

if [ $count_sales -gt 0 ]; then
    echo "moving $count_sales sale files ..."

    # moving .xls files into source_files/sales directory
    cp /mnt/c/Users/ingmg/Documents/*.xls /home/mgl/repos/coffee_shop_data_project/source_files/sales
fi

echo $YEAR