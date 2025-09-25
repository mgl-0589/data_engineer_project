import os
# import sys
# import json
from dotenv import load_dotenv
from file_manager import get_year, get_files
from xml_parser import XMLParser
from typing import Dict, List
import psycopg2


# sys.path.append('/opt/airflow/etl')


# load variables from .env
load_dotenv()

# global variables
HOME_DIRECTORY = os.getenv("AIRFLOW_HOME_DIR")
SOURCE_DIRECTORY = os.getenv("AIRFLOW_SOURCE_DIR")
INVOICE_XML_DIRECTORY = os.getenv("AIRFLOW_INVOICE_DIR")
# VOUCHER_XML_DIRECTORY = os.getenv("AIRFLOW_VOUCHER_DIR")


NAMESPACE = {"cfdi": "http://www.sat.gob.mx/cfd/4"} 
XML_SUFFIX = ".xml"
PDF_SUFFIX = ".pdf"
DETAILS_XPATH = "./cfdi:Conceptos//cfdi:Concepto"
TAXES_XPATH = "./cfdi:Conceptos//cfdi:Impuestos//cfdi:Traslados//cfdi:Traslado"


POSTGRES_HOST = os.getenv("AIRFLOW_POSTGRES_HOST")
POSTGRES_PORT = os.getenv("AIRFLOW_POSTGRES_PORT")
POSTGRES_DB = os.getenv("AIRFLOW_POSTGRES_DB")
POSTGRES_USER = os.getenv("AIRFLOW_POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("AIRFLOW_POSTGRES_PASSWORD")

ENV = os.getenv("ENVIRONMENT")





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


# def transform_into_dataframe(raw_data: List[Dict[str, str]]) -> pd.DataFrame:
#     """
#     Load data into a Dataframe

#     Args:
#         raw_data (str):
#     """
#     return pd.DataFrame(raw_data)


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
    """
    """
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


def create_stg_tables(conn: object) -> None:
    """
    """
    print("Creating stage table if not exist ...\n")
    try:
        cursor = conn.cursor()
        cursor.execute(f"""
            CREATE SCHEMA IF NOT EXISTS {ENV};
            
            CREATE TABLE IF NOT EXISTS {ENV}.stg_summary_data (
                id_stg_generals SERIAL PRIMARY KEY,
                file_name TEXT,
                date_creation TEXT,
                invoice_id TEXT,
                currency TEXT,
                subtotal TEXT,
                total TEXT,
                transmitter_id TEXT,
                transmitter_name TEXT,
                transmitter_zip_code TEXT,
                receiver_id TEXT,
                receiver_name TEXT,
                type TEXT,
                store TEXT,
                date_insertion TIMESTAMP DEFAULT NOW()
            );
            
            CREATE TABLE IF NOT EXISTS {ENV}.stg_details_data (
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
                       
            CREATE TABLE IF NOT EXISTS {ENV}.stg_taxes_data (
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
        print("Stage tables created successfully!\n")
    except psycopg2.Error as e:
        print(f"Failed to create stage tables: {e}")
        raise


def create_dim_tables(conn: object) -> None:
    """
    """
    print("Creating dimensional tables if not exist ...\n")
    try:
        cursor = conn.cursor()
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {ENV}.dim_suppliers (
                id_supplier SERIAL PRIMARY KEY,
                supplier_rfc VARCHAR(15),
                supplier_name VARCHAR(100),
                supplier_email VARCHAR(100),
                supplier_zip_code VARCHAR(15),
                date_insertion TIMESTAMP DEFAULT NOW()
            );
            
            CREATE TABLE IF NOT EXISTS {ENV}.dim_stores (
                id_store SERIAL PRIMARY KEY,
                receiver_rfc VARCHAR(15),
                receiver_name VARCHAR(100),
                store VARCHAR(3) CHECK (store IN ('TLQ', 'TRR', 'TPT')),
                date_insertion TIMESTAMP DEFAULT NOW()
            );
                       
            CREATE TABLE IF NOT EXISTS {ENV}.dim_status (
                id_status SERIAL PRIMARY KEY,
                status VARCHAR(15) CHECK (status IN ('proceso', 'revisión', 'pagada', 'cancelada')),
                date_insertion TIMESTAMP DEFAULT NOW()           
            );
                       
            CREATE TABLE IF NOT EXISTS {ENV}.dim_formality_types (
                id_formality_type SERIAL PRIMARY KEY,
                formality_type VARCHAR(15) CHECK (formality_type IN ('formal', 'informal'))
            );
                       
            CREATE TABLE IF NOT EXISTS {ENV}.dim_expense_types (
                id_expense_type SERIAL PRIMARY KEY,
                expense_type VARCHAR(15) CHECK (expense_type IN ('gasto', 'insumo', 'externo')),
                date_insertion TIMESTAMP DEFAULT NOW()           
            );
                       
            CREATE TABLE IF NOT EXISTS {ENV}.dim_payment_types (
                id_payment_type SERIAL PRIMARY KEY,
                payment_type VARCHAR(15) CHECK (payment_type IN ('transferencia', 'tarjeta', 'efectivo', 'cheque')),
                date_insertion TIMESTAMP DEFAULT NOW()           
            );
                       
            CREATE TABLE IF NOT EXISTS {ENV}.dim_unit_types (
                id_unit_type SERIAL PRIMARY KEY,
                product_key_unit VARCHAR(15),
                unit_type VARCHAR(5) CHECK (unit_type IN ('bolsa', 'pieza', 'servicio', 'litro', 'kilogramo', 'unidades')),
                date_insertion TIMESTAMP DEFAULT NOW()           
            );
                       
            CREATE TABLE IF NOT EXISTS {ENV}.dim_taxes (
                id_taxes SERIAL PRIMARY KEY,
                id_tax VARCHAR(15),
                type_factor VARCHAR(15),
                rate_or_share FLOAT,
                date_insertion TIMESTAMP DEFAULT NOW()           
            );

            CREATE TABLE IF NOT EXISTS {ENV}.dim_datetime (
                id_datetime SERIAL PRIMARY KEY,
                datetime TIMESTAMP,
                year INTEGER,
                month INTEGER,
                week INTEGER,
                day INTEGER,
                hour INTEGER,
                minut INTEGER,
                date_insertion TIMESTAMP DEFAULT NOW()
            );
        """)
        conn.commit()
        print("Dimensional tables created successfully!\n")
    except psycopg2.Error as e:
        print(f"Failed to create dimensional tables: {e}")
        raise


def create_fact_table(conn: object) -> None:
    """
    """
    print("Creating dimensional tables if not exist ...\n")
    try:
        cursor = conn.cursor()
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {ENV}.fact_invoice_products (
                id_invoice_product SERIAL PRIMARY KEY,
                id_supplier INTEGER,
                id_store INTEGER,
                id_status INTEGER,
                id_formality_type INTEGER,
                id_expense_type INTEGER,
                id_payment_type INTEGER,
                id_unit_type INTEGER,
                id_taxes INTEGER,
                id_datetime INTEGER,
                source_file TEXT,
                invoice_date_creation TIMESTAMP,
                invoice_number VARCHAR(100),
                product_key VARCHAR(50),
                product_id VARCHAR(50),
                product_quantity INTEGER,
                product_description TEXT,
                product_unit_value FLOAT,
                product_price FLOAT,
                product_discount FLOAT,
                product_amount_object VARCHAR(15),
                date_insertion TIMESTAMP DEFAULT NOW(),

                FOREIGN KEY (id_supplier) REFERENCES {ENV}.dim_suppliers(id_supplier) ON DELETE CASCADE,
                FOREIGN KEY (id_store) REFERENCES {ENV}.dim_stores(id_store) ON DELETE CASCADE,
                FOREIGN KEY (id_status) REFERENCES {ENV}.dim_status(id_status) ON DELETE CASCADE,
                FOREIGN KEY (id_formality_type) REFERENCES {ENV}.dim_formality_types(id_formality_type) ON DELETE CASCADE,
                FOREIGN KEY (id_expense_type) REFERENCES {ENV}.dim_expense_types(id_expense_type) ON DELETE CASCADE,
                FOREIGN KEY (id_payment_type) REFERENCES {ENV}.dim_payment_types(id_payment_type) ON DELETE CASCADE,
                FOREIGN KEY (id_unit_type) REFERENCES {ENV}.dim_unit_types(id_unit_type) ON DELETE CASCADE,
                FOREIGN KEY (id_taxes) REFERENCES {ENV}.dim_taxes(id_taxes) ON DELETE CASCADE,
                FOREIGN KEY (id_datetime) REFERENCES {ENV}.dim_datetime(id_datetime) ON DELETE CASCADE
            );
        """)
        conn.commit()
        print("Dimensional tables created successfully!\n")
    except psycopg2.Error as e:
        print(f"Failed to create dimensional tables: {e}")
        raise


def load_summary_data(conn: object, data: Dict[str, str]) -> None:
    """
    """
    try:
        cursor = conn.cursor()
        cursor.execute(f"""
            INSERT INTO {ENV}.stg_summary_data (
                file_name,
                date_creation,
                invoice_id,
                currency,
                subtotal,
                total,
                transmitter_id,
                transmitter_name,
                transmitter_zip_code,
                receiver_id,
                receiver_name,
                type,
                store,
                date_insertion
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        """, (
            f"{data.get("new_base_name")}.xml",
            data.get("date"),
            data.get("invoice_id"),
            data.get("currency"),
            data.get("subtotal"),
            data.get("total"),
            data.get("transmitter_id"),
            data.get("transmitter_name"),
            data.get("transmitter_zip_code"),
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
        cursor.execute(f"""
            INSERT INTO {ENV}.stg_details_data (
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
        cursor.execute(f"""
            INSERT INTO {ENV}.stg_taxes_data (
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


def main(year=None):
    
    # get year
    year = get_year(year)
    # print(year, end="\n\n")

    # creating the instance of XMLParser
    xml = XMLParser(HOME_DIRECTORY, SOURCE_DIRECTORY, NAMESPACE, XML_SUFFIX, PDF_SUFFIX)

    # defining paths of directories for invoices and vouchers
    invoice_dir = f"{HOME_DIRECTORY}/{INVOICE_XML_DIRECTORY}/{year}"
    # print(invoice_dir, end="\n\n")
        
    # getting list of invoices and vouchers
    xml_list = get_files(invoice_dir)
    # print(xml_list, end="\n\n")
        
    nested_details_data = [extract_xml_details(invoice_dir, invoice, xml, DETAILS_XPATH) for invoice in xml_list]
    # print(nested_details_data, end="\n\n")

    nested_taxes_data = [extract_xml_details(invoice_dir, invoice, xml, TAXES_XPATH) for invoice in xml_list]
    # print(nested_taxes_data, end="\n\n")

    # flattening data
    details_flattened_data = flatten_data(nested_details_data)
    taxes_flattened_data = flatten_data(nested_taxes_data)
    # print(details_flattened_data, end="\n\n")
    # print(taxes_flattened_data, end="\n\n")

    # extracting summary data for each invoice and voucher file
    summary_xml_data = [extract_xml_summary(invoice_dir, invoice, xml) for invoice in xml_list]
    # print(summary_xml_data, end="\n\n")

    conn = connect_to_db()
    create_stg_tables(conn)
    create_dim_tables(conn)
    create_fact_table(conn)

    try:
        for record in summary_xml_data:
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

