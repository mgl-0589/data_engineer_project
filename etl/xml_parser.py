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


# # global variables
# HOME_DIRECTORY = os.path.expanduser("~")
# SOURCE_DIRECTORY = os.getenv("SOURCE_DIRECTORY")
# NAMESPACE = {"cfdi": "http://www.sat.gob.mx/cfd/4"} 
# XML_SUFFIX = ".xml"
# PDF_SUFFIX = ".pdf"
# TAG_TRANSMITTER = "Emisor"
# TAG_RECEIVER = "Receptor"
# TAG_CONCEPTS = "Conceptos"


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

    def __init__(self, home_directory: str, source_directory: str, namespace: str, xml_suffix: str, pdf_suffix: str) -> None:
        self.directory = f"{home_directory}/{source_directory}"
        self.namespace = namespace
        self.xml_suffix = xml_suffix
        self.pdf_suffix = pdf_suffix

    
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

        try:
            invoice_id = root.get("Folio")
            if len(invoice_id) > 10:
                invoice_id = invoice_id[-10:]
        except KeyError:
            invoice_id = None
        
        # generating list of general transmitter and reciver data
        generals = [general.attrib for general in root]

        return {
            "source_xml_name": f"{file_name}",
            "source_pdf_name": f"{file_name.split(self.xml_suffix)[0]}{self.pdf_suffix}",
            "new_base_name": f"{root.get("Fecha").split("T")[0]}_{generals[0].get("Rfc")}_{invoice_id}_{root.get("TipoDeComprobante")}",
            "date": root.get("Fecha"),
            "invoice_id": invoice_id,
            "currency": root.get("Moneda"),
            "subtotal": root.get("SubTotal"),
            "total": root.get("Total"),
            "transmitter_id": generals[0].get("Rfc"),
            "transmitter_name": generals[0].get("Nombre"),
            "receiver_id": generals[1].get("Rfc"),
            "receiver_name": generals[1].get("Nombre"),
            "type": root.get("TipoDeComprobante")
        }
    

    def parse_xml_details(self, directory: str, file_name: str) -> Dict[str, str | List[str]]:

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
                [invoice.attrib for invoice in root.findall(f"./cfdi:Conceptos//cfdi:Concepto", self.namespace)]
            }


    def parse_xml_taxes(self, directory: str, file_name: str) -> Dict[str, str | List[str]]:

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
            "source_xml_name": file_name,
            "data": 
                [transfer.attrib for transfer in root.findall("./cfdi:Conceptos//cfdi:Impuestos//cfdi:Traslados//cfdi:Traslado", self.namespace)]
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