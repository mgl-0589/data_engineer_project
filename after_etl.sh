#!/bin/bash

set -o allexport
source ./.env
set +o allexport


YEAR=$(date +%Y%m%d | cut -c1-4)
date=$(date "+%Y%m%d")

LOCAL_DIRECTORY=$LOCAL_DIRECTORY
WSL_DIRECTORY=$HOME_DIRECTORY

count_invoices_pdf=$(ls -l $HOME_DIRECTORY/target_files/pdf/${YEAR}/*.pdf | wc -l)
count_invoices_xml=$(ls -l $HOME_DIRECTORY/target_files/xml/${YEAR}/*.xml | wc -l)
count_files=$(ls -l $HOME_DIRECTORY/target_files/tmp/*.* | wc -l)

if [ $count_invoices_pdf -gt 0 ]; then
    echo "moving $count_invoices_pdf file(s) ..."

    # moving invoices pdf
    mv $WSL_DIRECTORY/target_files/pdf/$YEAR/*.pdf $HOME_DIRECTORY/target_files/tmp/
fi

if [ $count_invoices_xml -gt 0 ]; then
    echo "moving $count_invoices_xml file(s) ..."

    # moving invoices xml
    mv $HOME_DIRECTORY/target_files/xml/$YEAR/*.xml $HOME_DIRECTORY/target_files/tmp/
fi