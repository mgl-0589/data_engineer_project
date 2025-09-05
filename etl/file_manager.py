import os
import shutil
import sys
from dotenv import load_dotenv
from zipfile import ZipFile
import xml.etree.ElementTree as ET
from datetime import datetime
from xml_parser import XMLParser


# load variables from .env
load_dotenv()

# global variables
HOME_DIRECTORY = os.getenv("HOME_DIRECTORY")
SOURCE_DIRECTORY = os.getenv("SOURCE_DIRECTORY")
INVOICE_XML_DIRECTORY = os.getenv("INVOICE_XML_DIRECTORY")
INVOICE_PDF_DIRECTORY = os.getenv("INVOICE_PDF_DIRECTORY")
VOUCHER_XML_DIRECTORY = os.getenv("VOUCHER_XML_DIRECTORY")
VOUCHER_PDF_DIRECTORY = os.getenv("VOUCHER_PDF_DIRECTORY")

NAMESPACE = {"cfdi": "http://www.sat.gob.mx/cfd/4"} 
XML_SUFFIX = ".xml"
PDF_SUFFIX = ".pdf"
ZIP_SUFFIX = ".zip"
TAG_TRANSMITTER = "Emisor"
TAG_RECEIVER = "Receptor"
TAG_CONCEPTS = "Conceptos"


# extract year function
def get_year(year: str|int =datetime.now().year) -> str:
    """
    Extract the year to specify from which source and target directories data will be extracted / located

    Args:
        year (str): year of interest
    Returns:
        year as a string
    """
    if len(sys.argv) > 1:
        return sys.argv[1]
    return str(year)


# get files
def get_files(source_directory: str) -> list[str]:
    """
    Extract all files names from source directory

    Args:
        source_directory (str): source from where file names will be extracted
    Returns:
        list of file names
    """
    try:
        return os.listdir(source_directory)
    except FileNotFoundError as e:
        print(f"File not found: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


# validate file name function
def validate_file_name(invoice_name: str) -> str:
    """
    Valdiate if the name has more than one '.' character, and remove all extra dots

    Args:
        invoice_name (str): name of the invoice file
    Returns:
        string with modifications
    """
    if invoice_name.count(".") > 1:
        return ".".join([invoice_name.rsplit(".", 1)[0].replace(".", "_"), invoice_name.rsplit(".", 1)[1]])
    else:
        return invoice_name


def remove_doble_extension(invoice_name: str) -> None:
    """
    Remove a doble extension on pdf file names
    
    Args:
        invoice_name
    Returns:
        None
    """
    if invoice_name.endswith(".xml.pdf"):
        return "".join([invoice_name.split(".xml")[0], invoice_name.split(".xml")[1]])
    else:
        return invoice_name
    

# change file name function
def change_file_name(source_directory: str, old_name: str, new_name: str) -> None:
    """
    Change the name of a file

    Args:
        source_directory (str): path of source directory where invoice file is located
        old_name (str): old name before extra-dot-validation
        new_name (str): new name post extra-dot_validation
    Returns:
        None
    """
    old_name = f"{source_directory}/{old_name}"
    new_name = f"{source_directory}/{new_name}"
    try:
        os.rename(old_name, new_name)
    except FileNotFoundError as e:
        print(f"File not found, error: {e}")
    except OSError as e:
        print(f"Error moving files: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


def copy_files(source_path: str, target_path: str, old_name: str, new_name: str, suffix: str) -> None:
    """
    Copy .xml and .pdf files to respective target folder (invoice/ | voucher/)
    
    Args:
        source_path (str): source directory path from where files will be extracted
        target_path (str): target directory path to where files will be copied
        old_name (str): current name of source file
        new_name (str): new name of file
        suffix (str): suffix indicating type of file (.xml | .pdf)
    Returns:
        None
    """
    # print(f"copying file from {source_path} into {target_path} ...")
    try:
        shutil.copy(f"{source_path}/{old_name}", f"{target_path}/{new_name}{suffix}")
    except FileExistsError as e:
        print(f"File in {source_path} already exists. Skipping copy")
    except FileNotFoundError as e:
        print(f"File not found, error: {e}")
    

def remove_files(source_path:str, file: str) -> None:
    """
    Remove all files in source directory after they are moved into respective folders

    Args:
        source_path (str): source directory path from where files will be removed
    Returns:
        None
    """
    try:
        os.remove(f"{source_path}/{file}")
    except OSError as e:
        print(f"Error removing files: {e}")


def unzip_files(source_path: str, file_name: str) -> None:
    """
    Unzip all files inside compressed folders (.zip)

    Args:
        source_path (str):
    Returns: 
        None
    """
    try:
        with ZipFile(f"{source_path}/{file_name}", 'r') as zip_file:
            zip_file.extractall(path=f"{source_path}")
    except FileNotFoundError as e:
        print(f"Error: zip file not found at {source_path}: {e}")
    except ZipFile.BadZipFile as e:
        print(f"Error: '{source_path}/{file_name}' is not a valid zip file: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")





##################################### testing #####################################
### add logging info, error, warning

def main():

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
    print(new_names_list, end='\n\n')
    

    # removing files without XML extension
    xml_invoice_list = [invoice for invoice in new_names_list if invoice.endswith(XML_SUFFIX)]
    print(xml_invoice_list, end="\n\n")


    # extract xml generals data
    invoice_generals_data = [xml_invoice.parse_xml_summary(f"{HOME_DIRECTORY}/{SOURCE_DIRECTORY}", invoice) for invoice in xml_invoice_list]
    print(invoice_generals_data, end="\n\n")


    # rename and copy files to respective target folder
    print(f"\nCopying files from source directory to respective target directory ...\n")
    for file in invoice_generals_data:

        files_source = f"{HOME_DIRECTORY}/{SOURCE_DIRECTORY}"
        
        if file.get("new_base_name").endswith("_I"):
            xml_target = f"{HOME_DIRECTORY}/{INVOICE_XML_DIRECTORY}/{year}"
            pdf_target = f"{HOME_DIRECTORY}/{INVOICE_PDF_DIRECTORY}/{year}"
            
            copy_files(f"{files_source}", xml_target, file.get("source_xml_name"), file.get("new_base_name"), XML_SUFFIX)
            copy_files(f"{files_source}", pdf_target, file.get("source_pdf_name"), file.get("new_base_name"), PDF_SUFFIX)
        
        elif file.get("new_base_name").endswith("_P"):
            xml_target = f"{HOME_DIRECTORY}/{VOUCHER_XML_DIRECTORY}/{year}"
            pdf_target = f"{HOME_DIRECTORY}/{VOUCHER_PDF_DIRECTORY}/{year}"

            copy_files(f"{HOME_DIRECTORY}/{SOURCE_DIRECTORY}", xml_target, file.get("source_xml_name"), file.get("new_base_name"), XML_SUFFIX)
            copy_files(f"{HOME_DIRECTORY}/{SOURCE_DIRECTORY}", pdf_target, file.get("source_pdf_name"), file.get("new_base_name"), PDF_SUFFIX)
            
    print(f"All files copied successfully!\n")


    # removing files from source directory
    print(f"\nRemoving files from source directory ...")

    [remove_files(f"{HOME_DIRECTORY}/{SOURCE_DIRECTORY}", file) for file in new_names_list]

    print(f"\nAll files removed successfully!\n")



main()