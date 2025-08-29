from typing import Dict, List
import pandas as pd



def extract_xml_summary(directory: str, file_name: str, xml_obj: object) -> Dict[str, str]:
    """
    Parse XML data to extract specific summary data from invoice

    Args:
        directory (str): path of directory where files will be parsed
        file_name (str): name of the XML file to be parsed
        xml_obj (object): XML object that uses "parse_xml_summary" method to extract summary data
    Returns:
        list of dictionary with summary data
    """
    return xml_obj.parse_xml_summary(directory, file_name)


def extract_xml_details(directory: str, file_name: str, xml_obj: object, xpath: str) -> Dict[str, str | List[Dict[str, str]]]:
    """
    Parse XML data to extract product details from invoice
    
    Args:
        directory (str): path of directory where files will be parsed
        file_name (str): name of the XML file to be parsed
        xml_obj (object): XML object that uses "parse_xml_details" method to extract details data
    Returns:
        List of dictionary with source file and data of products inside XML
    """
    return xml_obj.parse_xml_details(directory, file_name, xpath)


def transform_into_dataframe(raw_data: List[Dict[str, str]]) -> pd.DataFrame:
    """
    Load data into a Dataframe

    Args:
        raw_data (str):
        flatten
    """
    return pd.DataFrame(raw_data)


def flatten_data(nested_data: Dict[str, List[str]]) -> List[Dict[str, str]]:
    """
    Flatten nested data extracted from XML files
    
    Args:
        nested_data (str):
    Returns:
        Dictionary with flattened data
    """
    return [
        {'source': item['source'], **data_entry}
        for item in nested_data for data_entry in item['data']
    ]


def load_invoice_data():
    """
    """





###################################### testing ######################################

import os
from dotenv import load_dotenv
from file_manager import get_year, get_files
from xml_parser import XMLParser


# load variables from .env
load_dotenv()

# global variables
HOME_DIRECTORY = os.path.expanduser("~")
SOURCE_DIRECTORY = os.getenv("SOURCE_DIRECTORY")
INVOICE_XML_DIRECTORY = os.getenv("INVOICE_XML_DIRECTORY")
VOUCHER_XML_DIRECTORY = os.getenv("VOUCHER_XML_DIRECTORY")


NAMESPACE = {"cfdi": "http://www.sat.gob.mx/cfd/4"} 
XML_SUFFIX = ".xml"
PDF_SUFFIX = ".pdf"
DETAILS_XPATH = "./cfdi:Conceptos//cfdi:Concepto"
TAXES_XPATH = "./cfdi:Conceptos//cfdi:Impuestos//cfdi:Traslados//cfdi:Traslado"


# get year
year = get_year()

# creating the instance of XMLParser
xml = XMLParser(HOME_DIRECTORY, SOURCE_DIRECTORY, NAMESPACE, XML_SUFFIX, PDF_SUFFIX)

# defining paths of directories for invoices and vouchers
invoice_dir = f"{HOME_DIRECTORY}/{INVOICE_XML_DIRECTORY}/{year}"
voucher_dir = f"{HOME_DIRECTORY}/{VOUCHER_XML_DIRECTORY}/{year}"

# getting list of invoices and vouchers
invoices_list = get_files(invoice_dir)
vouchers_list = get_files(voucher_dir)

nested_details_data = [extract_xml_details(invoice_dir, invoice, xml, DETAILS_XPATH) for invoice in invoices_list]
# print(nested_details_data, end="\n\n")

nested_taxes_data = [extract_xml_details(invoice_dir, invoice, xml, TAXES_XPATH) for invoice in invoices_list]
# print(nested_taxes_data, end="\n\n")

# flattening data
details_flattened_data = flatten_data(nested_details_data)
taxes_flattened_data = flatten_data(nested_taxes_data)
print(details_flattened_data, end="\n\n")
print(taxes_flattened_data, end="\n\n")

# extracting summary data for each invoice and voucher file
summary_invoice_data = [extract_xml_summary(invoice_dir, invoice, xml) for invoice in invoices_list]
# print(summary_invoice_data, end="\n\n")

summary_voucher_data = [extract_xml_summary(voucher_dir, voucher, xml) for voucher in vouchers_list]
# print(summary_voucher_data, end="\n\n")

summary_data = summary_invoice_data + summary_voucher_data
print(summary_data)