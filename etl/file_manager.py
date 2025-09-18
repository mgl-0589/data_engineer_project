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


# extract year function
def get_year(year: str|int=None) -> str:
    """
    Extract the year to specify to which target directories data will be located

    Args:
        year (str): year of interest
    Returns:
        year as a string
    """
    if year:
        return str(year)
    return str(datetime.now().year)


def get_month(month: str|int=None) -> str:
    """
    Return the month as a two-digit string.
    If no month is provided, return the previous month.

    Args:
        month (str): month of interest
    Returns:
        month as a string with 2 digits
    """
    if month:
        return f"{int(month):02d}"
    return f"{(datetime.now().month - 1) or 12:02d}"


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


def transform_xls_into_xlsx(source_path: str, target_path: str, file_name: str, year: str, month:str) -> None:
    """
    """
    try:
        df = pd.read_excel(f"{source_path}/{file_name}", sheet_name=None)
        with pd.ExcelWriter(f"{target_path}/{year}_{month}.xlsx", engine="openpyxl") as writer:
            for sheet_name, sheet_df in df.items():
                sheet_df.to_excel(writer, sheet_name="venta_mensual", index=False)
    except FileNotFoundError as e:
        print(f"Error: xls file not found at {source_path}: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

