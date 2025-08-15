import os
import shutil
import sys
import xml.etree.ElementTree as ET


# global variables
HOME_PATH = os.path.expanduser("~")
BASE_PATH = "repos/coffee_shop_data_project"
SOURCE_DIRECTORY = "source_files"
TARGET_DIRECTORY = "target_files"
XML_INVOICE_FOLDER = "invoice_xml"
PDF_INVOICE_FOLDER = "invoice_pdf"
XML_VOUCHER_FOLDER = "voucher_xml"
PDF_VOUCHER_FOLDER = "voucher_pdf"


class PathDefiner:
    """Generate a path to define data source and target directories

    :param year: current year to be considered in directory creation

    :ivar year: year originally passed to when execution of script
    :ivar source_path: paht generator for directory of source files
    :ivar target_invoice_xml_path: path generator for target directory of invoice xml's 
    :ivar target_invoice_pdf_path: path generator for target directory of invoice pdf's
    :ivar target_voucher_xml_path: path generator for target directory of voucher xml's
    :ivar target_voucher_pdf_path: path generator for target directory of voucher pdf's
    """
    
    def __init__(self, year: str) -> None:
        self.year = year
        self.source_path = self._generate_source_path()
        self.target_invoice_xml_path = self._generate_target_path(XML_INVOICE_FOLDER)
        self.target_invoice_pdf_path = self._generate_target_path(PDF_INVOICE_FOLDER)
        self.target_voucher_xml_path = self._generate_target_path(XML_VOUCHER_FOLDER)
        self.target_voucher_pdf_path = self._generate_target_path(PDF_VOUCHER_FOLDER)


    def _generate_source_path(self) -> str:
        return os.path.join(HOME_PATH, BASE_PATH, SOURCE_DIRECTORY)


    def _generate_target_path(self, folder) -> str:
        return os.path.join(HOME_PATH, BASE_PATH, TARGET_DIRECTORY, folder, self.year)



