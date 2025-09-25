import os
import shutil
import sys
import json
import pandas as pd
from dotenv import load_dotenv
from zipfile import ZipFile
import xml.etree.ElementTree as ET
from datetime import datetime
from xml_parser import XMLParser
from file_manager import *


# load variables from .env
load_dotenv()

# global variables
HOME_DIRECTORY = os.getenv("HOME_DIRECTORY")
SOURCE_DIRECTORY = os.getenv("SOURCE_XML_DIRECTORY")
SOURCE_SALES_DIRECTORY = os.getenv("SOURCE_XLS_DIRECTORY")
XML_DIRECTORY = os.getenv("XML_DIRECTORY")
PDF_DIRECTORY = os.getenv("PDF_DIRECTORY")
XLSX_DIRECTORY = os.getenv("XLSX_DIRECTORY")


NAMESPACE = {"cfdi": "http://www.sat.gob.mx/cfd/4"} 
XML_SUFFIX = ".xml"
PDF_SUFFIX = ".pdf"
ZIP_SUFFIX = ".zip"
TAG_TRANSMITTER = "Emisor"
TAG_RECEIVER = "Receptor"
TAG_CONCEPTS = "Conceptos"

STORE_MAP = json.loads(os.getenv("STORE_MAP"))



##################################### testing #####################################
### add logging info, error, warning

def invoice_main():

    print(f"\nExecutable: {sys.executable}\n")

    # extracting year
    year = get_year()
    # print(year, end='\n\n')

    # create XMLParser instance
    xml_invoice = XMLParser(HOME_DIRECTORY, SOURCE_DIRECTORY, NAMESPACE, XML_SUFFIX, PDF_SUFFIX)
    # print(xml_invoice.directory)


    # getting list of file names
    original_names_list = get_files(f"{HOME_DIRECTORY}/{SOURCE_DIRECTORY}")
    # print(original_names_list, end='\n\n')


    # getting list of zip files
    zip_files_list = [invoice for invoice in original_names_list if invoice.endswith(ZIP_SUFFIX)]
    # print(zip_files_list, end="\n\n")


    # extracting files from compressed file
    for zip_file in zip_files_list:
        unzip_files(f"{HOME_DIRECTORY}/{SOURCE_DIRECTORY}", zip_file)
        remove_files(f"{HOME_DIRECTORY}/{SOURCE_DIRECTORY}", zip_file)


    # removing double extension
    deduped_ext_names_list = [*map(remove_doble_extension, original_names_list)]
    # print(deduped_ext_names_list, end="\n\n")


    # validating file names
    validated_names_list = [*map(validate_file_name, deduped_ext_names_list)]
    # print(validated_names_list, end='\n\n')


    # combine the original and modified names in a list of tuples
    names_combined_list = [(old_name, new_name) for old_name, new_name in zip(original_names_list, validated_names_list) if old_name != new_name]
    # print(names_combined_list, end="\n\n")


    # change the name of files that has been validated to remove extra "." characters
    for old_name, new_name in names_combined_list:
        
        if old_name != new_name:
            change_file_name(f"{HOME_DIRECTORY}/{SOURCE_DIRECTORY}", old_name, new_name)


    # getting list of file names
    new_names_list = get_files(f"{HOME_DIRECTORY}/{SOURCE_DIRECTORY}")
    # print(new_names_list, end='\n\n')
    

    # filtering out files without XML extension
    xml_invoice_list = [invoice for invoice in new_names_list if invoice.endswith(XML_SUFFIX)]
    # print(xml_invoice_list, end="\n\n")


    # extract xml generals data
    invoice_generals_data = [xml_invoice.parse_xml_summary(f"{HOME_DIRECTORY}/{SOURCE_DIRECTORY}", invoice) for invoice in xml_invoice_list]
    # print(invoice_generals_data, end="\n\n")


    # rename and copy files to respective target folder
    print(f"\nCopying files from source directory to respective target directory ...\n")
    for file in invoice_generals_data:

        files_source = f"{HOME_DIRECTORY}/{SOURCE_DIRECTORY}"
        
        xml_target = f"{HOME_DIRECTORY}/{XML_DIRECTORY}/{year}"
        pdf_target = f"{HOME_DIRECTORY}/{PDF_DIRECTORY}/{year}"
            
        copy_files(f"{files_source}", xml_target, file.get("source_xml_name"), file.get("new_base_name"), XML_SUFFIX)
        copy_files(f"{files_source}", pdf_target, file.get("source_pdf_name"), file.get("new_base_name"), PDF_SUFFIX)
            
    print(f"All files copied successfully!\n")


    # removing files from source directory
    print(f"\nRemoving files from source directory ...")

    [remove_files(f"{HOME_DIRECTORY}/{SOURCE_DIRECTORY}", file) for file in new_names_list]

    print(f"\nAll files removed successfully!\n")



def sales_main():

    #getting year
    year = get_year()
    # print(year, end="\n\n")

    #getting month
    month = get_month()
    # print(month)

    # getting list of xls files
    list_xls = get_files(f"{HOME_DIRECTORY}/{SOURCE_SALES_DIRECTORY}")
    # print(list_xls, end="\n\n")

    [transform_xls_into_xlsx(f"{HOME_DIRECTORY}/{SOURCE_SALES_DIRECTORY}", f"{HOME_DIRECTORY}/{XLSX_DIRECTORY}", file_name, year, month) for file_name in list_xls]
    # list_xlsx = get_files(f"{HOME_DIRECTORY}/{XLSX_DIRECTORY}")
    # print(list_xlsx, end="\n\n")

    # removing xls files from source directory
    [remove_files(f"{HOME_DIRECTORY}/{SOURCE_SALES_DIRECTORY}", file_name) for file_name in list_xls]



invoice_main()
sales_main()