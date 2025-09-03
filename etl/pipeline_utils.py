import os
from dotenv import load_dotenv
from file_manager import get_year, get_files
from xml_parser import XMLParser
import pandas as pd
from typing import Dict, List
# import pandas as pd
import psycopg2


# load variables from .env
load_dotenv()

# global variables
HOME_DIRECTORY = os.getenv("HOME_DIRECTORY")
SOURCE_DIRECTORY = os.getenv("SOURCE_DIRECTORY")
INVOICE_XML_DIRECTORY = os.getenv("INVOICE_XML_DIRECTORY")
VOUCHER_XML_DIRECTORY = os.getenv("VOUCHER_XML_DIRECTORY")


NAMESPACE = {"cfdi": "http://www.sat.gob.mx/cfd/4"} 
XML_SUFFIX = ".xml"
PDF_SUFFIX = ".pdf"
DETAILS_XPATH = "./cfdi:Conceptos//cfdi:Concepto"
TAXES_XPATH = "./cfdi:Conceptos//cfdi:Impuestos//cfdi:Traslados//cfdi:Traslado"


POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")




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


def connect_to_db():
    print("\nConnecting to the PostgreSQL database ...\n")
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        return conn
    except psycopg2.Error as e:
        print(f"Database connection failed: {e}")
        raise


def create_stg_tables(conn):
    print("Creating table if not exists ...\n")
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS dev;
            
            CREATE TABLE IF NOT EXISTS dev.stg_summary_data (
                id_stg_generals SERIAL PRIMARY KEY,
                file_name TEXT,
                date_creation TEXT,
                invoice_id TEXT,
                currency TEXT,
                subtotal TEXT,
                total TEXT,
                transmitter_id TEXT,
                transmitter_name TEXT,
                receiver_id TEXT,
                receiver_name TEXT,
                type TEXT,
                store TEXT,
                date_insertion TIMESTAMP DEFAULT NOW()
            );
            
            CREATE TABLE IF NOT EXISTS dev.stg_details_data (
                id_stg_details SERIAL PRIMARY KEY,
                source TEXT,
                product_service_key TEXT,
                product_id TEXT,
                quantity TEXT,
                unit_key TEXT,
                units TEXT,
                description TEXT,
                unit_value TEXT,
                amount TEXT,
                discount TEXT,
                amount_object TEXT,
                date_insertion TIMESTAMP DEFAULT NOW()
            );
                       
            CREATE TABLE IF NOT EXISTS dev.stg_taxes_data (
                id_stg_taxes SERIAL PRIMARY KEY,
                source TEXT,
                base TEXT,
                amount TEXT,
                tax_id TEXT,
                type_factor TEXT,
                rate_or_share TEXT,
                date_insertion TIMESTAMP DEFAULT NOW()
            );
        """)
        conn.commit()
        print("Tables created successfully!\n")
    except psycopg2.Error as e:
        print(f"Failed to create tables: {e}")
        raise


def load_summary_data(conn: object, data: Dict[str, str]) -> None:
    """
    """
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO dev.stg_summary_data (
                file_name,
                date_creation,
                invoice_id,
                currency,
                subtotal,
                total,
                transmitter_id,
                transmitter_name,
                receiver_id,
                receiver_name,
                type,
                store,
                date_insertion
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        """, (
            f"{data.get("new_base_name")}.xml",
            data.get("date"),
            data.get("invoice_id"),
            data.get("currency"),
            data.get("subtotal"),
            data.get("total"),
            data.get("transmitter_id"),
            data.get("transmitter_name"),
            data.get("receiver_id"),
            data.get("receiver_name"),
            data.get("type"),
            data.get("store")
        ))
        conn.commit()
        # print("Data inserted successfully!\n")
    except psycopg2.Error as e:
        print(f"Error inserting data into database: {e}")
    except Exception as e:
        print(f"Failed to insert records: {e}")
        raise


def load_details_data(conn: object, data: Dict[str, str]) -> None:
    """
    """
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO dev.stg_details_data (
                source,
                product_service_key,
                product_id,
                quantity,
                unit_key,
                units,
                description,
                unit_value,
                amount,
                discount,
                amount_object,
                date_insertion
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        """, (
            data.get("source"),
            data.get("ClaveProdServ"),
            data.get("NoIdentificacion"),
            data.get("Cantidad"),
            data.get("ClaveUnidad"),
            data.get("Unidad"),
            data.get("Descripcion"),
            data.get("ValorUnitario"),
            data.get("Importe"),
            data.get("Descuento"),
            data.get("ObjetoImp")
        ))
        conn.commit()
        # print("Data inserted successfully!\n")
    except psycopg2.Error as e:
        print()
    except Exception as e:
        print(f"Failed to insert records: {e}")
        raise


def load_taxes_data(conn: object, data: Dict[str, str]) -> None:
    """
    """
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO dev.stg_taxes_data (
                source,
                base,
                amount,
                tax_id,
                type_factor,
                rate_or_share,
                date_insertion
            ) VALUES (%s, %s, %s, %s, %s, %s, NOW())
        """, (
            data.get("source"),
            data.get("Base"),
            data.get("Importe"),
            data.get("Impuesto"),
            data.get("TipoFactor"),
            data.get("TasaOCuota")
        ))
        conn.commit()
        # print("Data inserted successfully!\n")
    except psycopg2.Error as e:
        print()
    except Exception as e:
        print(f"Failed to insert records: {e}")
        raise


def main():
    
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
    # print(details_flattened_data, end="\n\n")
    # print(taxes_flattened_data, end="\n\n")

    # extracting summary data for each invoice and voucher file
    summary_invoice_data = [extract_xml_summary(invoice_dir, invoice, xml) for invoice in invoices_list]
    # print(summary_invoice_data, end="\n\n")

    summary_voucher_data = [extract_xml_summary(voucher_dir, voucher, xml) for voucher in vouchers_list]
    # print(summary_voucher_data, end="\n\n")

    summary_data = summary_invoice_data + summary_voucher_data
    # print(summary_data)

    conn = connect_to_db()
    create_stg_tables(conn)

    try:
        for record in summary_data:
            load_summary_data(conn, record)
        print("\nSummary data inserted successfully!\n")

        for record in details_flattened_data:
            load_details_data(conn, record)
        print("\nDetails data inserted successfully!\n")

        for record in taxes_flattened_data:
            load_taxes_data(conn, record)
        print("\nTaxes data inserted successfully!\n")
        
    except Exception as e:
        print(f"An error occurred during execution: {e}")
    finally:
        if 'conn' in locals():
            conn.close()
            print("Database connection closed.\n")

