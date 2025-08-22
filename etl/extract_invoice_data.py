import os
import shutil
import sys
from dotenv import load_dotenv
import xml.etree.ElementTree as ET
from datetime import datetime
from xml_parser import XMLParser
from file_manager import get_year, get_files
import logging


# load variables from .env
load_dotenv()


# define global variables
XML_SUFFIX = ".xml"
PDF_SUFFIX = ".pdf"
NS = {"cfdi": "http://www.sat.gob.mx/cfd/4"}


# extract year
def define_year(year: str|int =datetime.now().year) -> str:
    """
    Extract the year to specify from which source and target directories data will be extracted / located

    Args:
        year (str): year of interest
    """
    if len(sys.argv) > 1:
        return sys.argv[1]
    return str(year)






##################################### testing #####################################
def main():

    print(f"\nExecutable: {sys.executable}\n")
    
    
    # extract year
    year = define_year()