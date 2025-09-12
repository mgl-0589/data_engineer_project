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
    date: datetime
    invoice_id: Optional[str]
    currency: Optional[str]
    subtotal: Optional[str]
    total: Optional[str]
    transmitter_id: str
    transmitter_name: Optional[str]
    transmitter_zip_code: Optional[str]
    receiver_id: str
    receiver_name: Optional[str]
    type: str
    store: str = Field(default="TLQ")
    xml_suffix: str = Field(default=".xml")
    pdf_suffix: str = Field(default=".pdf")

    @field_validator("invoice_id", mode="before")
    def trim_invoice_id(cls, v: Optional[str], values) -> Optional[str]:
        """
        - If invoice_id is longer than 10 characters, keeps the las 10
        - If invoice_id is None, generate fallback from date -> 'YYYYMMDD-SF'
        """
        if v:
            return v[-10:] if len(v) > 10 else v
        
        date_val = values.get("date")
        if isinstance(date_val, datetime):
            return date_val.strftime("%Y%m%d") + "-SF"
        
        return "00000000-SF"
    
    # @field_validator("subtotal", "total", mode="before")
    # def convert_to_float(cls, v):
    #     """
    #     Convert numeric string to float
    #     """
    #     return float(v) if v is not None else None
    
    @model_validator(mode="after")
    def build_fields(cls, values) -> "InvoiceSummary":
        """
        Build derived fields:
            - source_pdf_name
            - new_base_name
            - store from transmitter_id
        Ensure derived fields (source_pdf_name, new_source_name) are always genrated from validated fields
        """
        # generate pdf name base on XML name
        if values.source_xml_name.endswith(values.xml_suffix):
            base_name = values.source_xml_name[: -len(values.xml_suffix)]
        else:
            base_name = values.source_xml_name
        
        values.source_pdf_name = f"{base_name}{values.pdf_suffix}"

        # pick values from mapping
        values.store = STORE_MAP.get(values.transmitter_id, values.store)

        # build new base name using validated invoice_id
        values.new_base_name = (
            f"{values.date.strftime('%Y-%m-%d')}_"
            f"{values.transmitter_id}_"
            f"{values.invoice_id}_"
            f"{values.store}_"
            f"{values.type}"
        )
        return values


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

        summary_model = InvoiceSummary(
            source_xml_name = file_name,
            date = root.get("Fecha"),
            invoice_id = root.get("Folio"),
            currency = root.get("Moneda"),
            subtotal = root.get("SubTotal"),
            total = root.get("Total"),
            transmitter_id = generals[0].get("Rfc"),
            transmitter_name = generals[0].get("Nombre"),
            transmitter_zip_code = root.get("LugarExpedicion"),
            receiver_id = generals[1].get("Rfc"),
            receiver_name = generals[1].get("Nombre"),
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

