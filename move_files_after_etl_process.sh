#!/bin/bash

YEAR=$(date +%Y%m%d | cut -c1-4)
date=$(date "+%Y%m%d")

LOCAL_DIRECTORY="/mnt/c/Users/ingmg/Documents"
WSL_DIRECTORY="/home/mgl/repos/coffee_shop_data_project"

count_invoices_pdf=$(ls -l $WSL_DIRECTORY/target_files/pdf/${YEAR}/*.pdf | wc -l)
count_invoices_xml=$(ls -l $WSL_DIRECTORY/target_files/xml/${YEAR}/*.xml | wc -l)
count_files=$(ls -l $WSL_DIRECTORY/target_files/tmp/*.* | wc -l)

if [ $count_invoices_pdf -gt 0 ]; then
    echo "moving $count_invoices_pdf file(s) ..."

    # moving invoices pdf
    mv $WSL_DIRECTORY/target_files/pdf/$YEAR/*.pdf $WSL_DIRECTORY/target_files/tmp/
fi

if [ $count_invoices_xml -gt 0 ]; then
    echo "moving $count_invoices_xml file(s) ..."

    # moving invoices xml
    mv $WSL_DIRECTORY/target_files/xml/$YEAR/*.xml $WSL_DIRECTORY/target_files/tmp/
fi


