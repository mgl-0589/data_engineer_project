
from typing import Dict, List



def extract_xml_summary(directory: str, file_name: str, xml_obj: object) -> Dict[str, str]:
    """
    Extract specific summary data from XML file

    Args:
        directory (str): path of directory where files will be parsed
        file_name (str): name of the XML file to be parsed
        xml_obj (object): XML object that uses "parse_xml_summary" method to extract summary data
    Returns:
        list of dictionary with summary data
    """
    return xml_obj.parse_xml_summary(directory, file_name)


def extract_xml_details(directory: str, file_name: str, xml_obj: object) -> Dict[str, str | List[str]]:
    """
    
    Args:
        directory (str):
        file_name (str):
        xml_obj (object): 
    Returns:
        List of dictionary with source file and data of products inside XML
    """
    return xml_obj.parse_xml_details(directory, file_name)


def transform_invoice_data(raw_data: List[Dict[str, str]]):
    """
    Flatten data to be loaded into pd.DataFrame
    """



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

# extracting summary data for each invoice and voucher file
raw_summary_invoice_data = [extract_xml_summary(invoice_dir, invoice, xml) for invoice in invoices_list]
# print(raw_summary_invoice_data, end="\n\n")

raw_summary_voucher_data = [extract_xml_summary(voucher_dir, voucher, xml) for voucher in vouchers_list]
# print(raw_summary_voucher_data, end="\n\n")

raw_details_invoice_data = [extract_xml_details(invoice_dir, invoice, xml) for invoice in invoices_list]
# print(raw_details_invoice_data, end="\n\n")

raw_details_invoice_data = [extract_xml_details(voucher_dir, voucher, xml) for voucher in vouchers_list]
# print(raw_details_invoice_data, end="\n\n")

raw_taxes_invoice_data = [xml.parse_xml_taxes(invoice_dir, invoice) for invoice in invoices_list]
# print(raw_taxes_invoice_data, end="\n\n")

