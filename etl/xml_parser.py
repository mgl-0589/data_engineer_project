from dotenv import load_dotenv
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import List, Optional
import logging
import os
import json
from pydantic import BaseModel, Field, field_validator, model_validator

# load variables from .env
load_dotenv()

STORE_MAP = json.loads(os.getenv("STORE_MAP"))



class InvoiceSummary(BaseModel):
    source_xml_name: str
    source_pdf_name: Optional[str] = None
    new_base_name: Optional[str] = None
    date: str
    invoice_id: Optional[str]
    currency: Optional[str]
    subtotal: Optional[str]
    total: Optional[str]
    transmitter_id: Optional[str]
    transmitter_name: Optional[str]
    transmitter_zip_code: Optional[str]
    receiver_id: str
    receiver_name: Optional[str]
    type: str
    store: str = Field(default="TLQ")
    xml_suffix: str = Field(default=".xml")
    pdf_suffix: str = Field(default=".pdf")

    @field_validator("invoice_id", mode="before")
    def trim_invoice_id(cls, v: Optional[str], info) -> Optional[str]:
        """
        - If invoice_id is longer than 10 characters, keeps the las 10
        - If invoice_id is None, generate fallback from date -> 'YYYYMMDD-SF'
        """
        if v:
            return v[-10:] if len(v) > 10 else v
        
        else:
            return f"SF-{info.data.get("date").split("T")[0].replace("-", "")}" 
    
    @model_validator(mode="after")
    def build_fields(self) -> "InvoiceSummary":
        """
        Build derived fields:
            - source_pdf_name
            - new_base_name
            - store from transmitter_id
        Ensure derived fields (source_pdf_name, new_source_name) are always genrated from validated fields
        """
        # generate pdf name base on XML name
        if self.source_xml_name.endswith(self.xml_suffix):
            base_name = self.source_xml_name[: -len(self.xml_suffix)]
        else:
            base_name = self.source_xml_name
        
        self.source_pdf_name = f"{base_name}{self.pdf_suffix}"

        # pick values from mapping
        self.store = STORE_MAP.get(self.transmitter_id, self.store)

        # build new base name using validated invoice_id
        self.new_base_name = (
            f"{self.date.split("T")[0]}_"
            f"{self.transmitter_id}_"
            f"{self.invoice_id}_"
            f"{self.store}_"
            f"{self.type}"
        )
        return self


class InvoiceDetail(BaseModel):
    source: str
    data: List[dict]


class XMLParser:
    """
    Parse XML files in the specified directory and extract attributes from a given tag
    """

    def __init__(self, home_directory: str, source_directory: str, namespace: dict, xml_suffix: str=".xml", pdf_suffix: str=".pdf") -> None:
        self.directory = f"{home_directory}/{source_directory}"
        self.namespace = namespace
        self.xml_suffix = xml_suffix
        self.pdf_suffix = pdf_suffix

    
    def parse_xml_summary(self, directory: str, file_name: str) -> InvoiceSummary:

        try:
            xml_tree = ET.parse(f"{directory}/{file_name}")
            root = xml_tree.getroot()
        except FileNotFoundError as e:
            logging.error(f"File not found: {file_name} - {e}")
            raise
        except ET.ParseError as e:
            logging.error(f"XML parsing error in {file_name}: {e}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error in {file_name}: {e}")
            raise
        
        # generating list of general transmitter and reciver data
        generals = [general.attrib for general in root]

        # determine the indices based on xml type or elements retrieved in generals.attrib
        type = root.get("TipoDeComprobante")
        if type == "E" or generals[0].get("Emisor") is None:
            transmitter_idx = 1
            receiver_idx = 2
        else:
            transmitter_idx = 0
            receiver_idx = 1

        summary_model = InvoiceSummary(
            source_xml_name = file_name,
            date = root.get("Fecha"),
            invoice_id = root.get("Folio"),
            currency = root.get("Moneda"),
            subtotal = root.get("SubTotal"),
            total = root.get("Total"),
            transmitter_id = generals[transmitter_idx].get("Rfc"),
            transmitter_name = generals[transmitter_idx].get("Nombre"),
            transmitter_zip_code = root.get("LugarExpedicion"),
            receiver_id = generals[receiver_idx].get("Rfc"),
            receiver_name = generals[receiver_idx].get("Nombre"),
            type = root.get("TipoDeComprobante"),
            # store = store,
        )

        return summary_model.model_dump()
    

    def parse_xml_details(self, directory: str, file_name: str, xpath: str) -> InvoiceDetail:

        try:
            xml_tree = ET.parse(f"{directory}/{file_name}")
            root = xml_tree.getroot()
        except FileNotFoundError as e:
            logging.error(f"File not found: {file_name} - {e}")
            raise
        except ET.ParseError as e:
            logging.error(f"XML parsing error in {file_name}: {e}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error in {file_name}: {e}")
            raise

        details_model = InvoiceDetail(
            source = file_name,
            data = [invoice.attrib for invoice in root.findall(xpath, self.namespace)],
        )

        return details_model.model_dump()

