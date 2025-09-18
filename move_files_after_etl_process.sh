#!/bin/bash

YEAR=$(date +%Y%m%d | cut -c1-4)

count_invoices_pdf=$(ls -l /home/mgl/repos/coffee_shop_data_project/target_files/pdf/$YEAR/*.pdf | wc -l)
count_files=$(ls -l /home/mgl/repos/coffee_shop_data_project/target_files/tmp/*.* | wc -l)

if [ $count_invoices_pdf -gt 0 ]; then
    echo "moving $count_invoices_pdf file(s) ..."

    # moving invoices pdf
    mv /home/mgl/repos/coffee_shop_data_project/target_files/pdf/$YEAR/*.pdf /home/mgl/repos/coffee_shop_data_project/target_files/tmp/
fi

if [ $count_files -gt 0 ]; then
    echo "moving $count_files file(s) ..."

    # compressing files
    
    # moving compressed file
    cp /home/mgl/repos/coffee_shop_data_project/target_files/tmp
fi

echo $YEAR
