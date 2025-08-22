import os
import glob
import sys
from dotenv import load_dotenv
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import List, Dict
import logging


# load variables from .env
load_dotenv()


# global variables
HOME_DIRECTORY = os.path.expanduser("~")
SOURCE_DIRECTORY = os.getenv("SOURCE_DIRECTORY")
NAMESPACE = {"cfdi": "http://www.sat.gob.mx/cfd/4"} 
XML_SUFFIX = ".xml"
PDF_SUFFIX = ".pdf"
TAG_TRANSMITTER = "Emisor"
TAG_RECEIVER = "Receptor"
TAG_CONCEPTS = "Conceptos"


class XMLParser:
    """
    Parse XML files in the specified directory and extract attributes from a given tag

    :param directory:
    
    :ivar directory:
    :ivar tag_name:
    :ivar namespace:
    :ivar xml_suffix:
    :ivar pdf_suffix:
    """

    def __init__(self, home_directory, source_directory, namespace, xml_suffix, pdf_suffix, tag_transmitter, tag_receiver, tag_conpctps) -> None:
        self.directory = f"{home_directory}/{source_directory}"
        self.namespace = namespace
        self.xml_suffix = xml_suffix
        self.pdf_suffix = pdf_suffix
        self.tag_transmitter = tag_transmitter
        self.tag_receiver = tag_receiver
        self.tag_concepts = tag_conpctps
        # self.files_list = self._get_xml_files()

    
    # def get_files(self) -> List[str]:
    #     return os.listdir(self.directory)

    
    def parse_xml_summary(self, directory: str, file_name: str) -> dict[str: str]:

        try:
            xml_tree = ET.parse(f"{directory}/{file_name}")
            root = xml_tree.getroot()
        
        except FileNotFoundError as e:
            logging.error(f"File not found: {file_name} - {e}")
        except ET.ParseError as e:
            logging.error(f"XML parsing error in {file_name}: {e}")
        except Exception as e:
            logging.error(f"Unexpected error in {file_name}: {e}")

        # getting summary invoice data
        generals = root.attrib
        date = generals["Fecha"]
        transaction_type = generals["TipoDeComprobante"]
        currency = generals["Moneda"]
        subtotal = generals["SubTotal"]
        total = generals["Total"]

        try:
            invoice_id = generals["Folio"]
            if len(invoice_id) > 10:
                invoice_id = invoice_id[-10:]
        except KeyError:
            invoice_id = None

        # getting transmitter data
        generals_transmitter = root.find(f"cfdi:{self.tag_transmitter}", self.namespace)
        transmitter_id = generals_transmitter.attrib["Rfc"]
        transmitter_name = generals_transmitter.attrib["Nombre"]

        # getting receiver data
        generals_receiver = root.find(f"cfdi:{self.tag_receiver}", self.namespace)
        receiver_id = generals_receiver.attrib["Rfc"]
        receiver_name = generals_receiver.attrib["Nombre"]

        return {
            "source_xml_name": f"{file_name}",
            "source_pdf_name": f"{file_name.split(self.xml_suffix)[0]}{self.pdf_suffix}",
            "new_base_name": f"{date.split("T")[0]}_{transmitter_id}_{invoice_id}_{transaction_type}",
            "date": date,
            "invoice_id": invoice_id,
            "currency": currency,
            "subtotal": subtotal,
            "total": total,
            "transmitter_id": transmitter_id,
            "transmitter_name": transmitter_name,
            "receiver_id": receiver_id,
            "receiver_name": receiver_name
        }
    

    def parse_xml_details(self, directory: str, file_name: str) -> List[str]:

        try:
            xml_tree = ET.parse(f"{directory}/{file_name}")
            root = xml_tree.getroot()
        except FileNotFoundError as e:
            logging.error(f"File not found: {file_name} - {e}")
        except ET.ParseError as e:
            logging.error(f"XML parsing error in {file_name}: {e}")
        except Exception as e:
            logging.error(f"Unexpected error in {file_name}: {e}")

        return {
            "source": file_name,
            "data":
                [invoice.attrib for invoice in root.find(f"cfdi:{self.tag_concepts}", self.namespace)]
            }


############################# testing #############################

# def main():

#     # print(f"\nExecutable: {sys.executable}\n")

   
#     # create object instance 
#     xml_data = XMLParser(HOME_DIRECTORY, SOURCE_DIRECTORY, NAMESPACE, XML_SUFFIX, PDF_SUFFIX, TAG_TRANSMITTER, TAG_RECEIVER, TAG_CONCEPTS)
#     original_list = xml_data.get_files()
#     # print(original_list, end="\n\n")


#     # keep with XML files only
#     xml_invoice_list = [invoice for invoice in original_list if invoice.endswith(XML_SUFFIX)]
#     # print(xml_invoice_list, end="\n\n")


#     # extract xml generals data
#     invoice_generals_data = [xml_data.parse_xml_summary(f"{HOME_DIRECTORY}/{SOURCE_DIRECTORY}", invoice) for invoice in xml_invoice_list]
#     # print(invoice_generals_data, end="\n\n")


#     # extract xml details data
#     invoice_details_data = [xml_data.parse_xml_details(f"{HOME_DIRECTORY}/{SOURCE_DIRECTORY}", invoice) for invoice in xml_invoice_list]
#     # print(invoice_details_data[1], end="\n\n")



# main()