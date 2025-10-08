import os
from dotenv import load_dotenv
from file_manager import get_year, get_files
import pandas as pd
from typing import Dict, List
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
# import numpy as np


from file_manager import get_year, get_files


# load variables from .env
load_dotenv()

# global variables
HOME_DIRECTORY = os.getenv("AIRFLOW_HOME_DIR")
SOURCE_DIRECTORY = os.getenv("AIRFLOW_SOURCE_DIR")
INVOICE_XML_DIRECTORY = os.getenv("AIRFLOW_INVOICE_DIR")
SALES_XLSX_DIRECTORY = os.getenv("AIRFLOW_SALES_DIR")

POSTGRES_HOST = os.getenv("AIRFLOW_POSTGRES_HOST")
POSTGRES_PORT = os.getenv("AIRFLOW_POSTGRES_PORT")
POSTGRES_DB = os.getenv("AIRFLOW_POSTGRES_DB")
POSTGRES_USER = os.getenv("AIRFLOW_POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("AIRFLOW_POSTGRES_PASSWORD")

ENV = os.getenv("ENVIRONMENT")

# columns to drop
columns_to_drop = [
    "ARQUEO CONTABILIZADO", 
    "ANTICIPOS/VALES GENERADOS", 
    "COBROS", 
    "PAGOS", 
    "VALES IMPUTADOS", 
    "VALES GENERADOS", 
    "A INGRESAR", 
    "CAMBIO INICIAL", 
    "CALCULADO METALICO", 
    "DECLARADO", 
    "DESCUADRE METALICO", 
    "COBROS NO METALICOS", 
    "E.CUENTA NO METALICAS", 
    "PAGOS NO METALICOS", 
    "NUM.COMENSALES", 
    "MEDIA X COMENSAL", 
    "IMPUESTOS", 
    "TOTAL NETO"
]

# columns to keep
columns_ordered = [
    "DIA",
    "VENTAS", 
    "DEVOLUCIONES", 
    "VENTAS METALICO", 
    "TARJETA DE DEBITO", 
    "TARJETA DE CREDITO", 
    "UBER EATS", 
    "RAPPI", 
    "TRANSFERENCIA", 
    "NUM.TICKETS", 
    "MEDIA X TICKET", 
    "NUMERO DEVOLUCIONES", 
    "IMPORTE DEVOLUCIONES", 
    "NUMERO INVITACIONES", 
    "IMPORTE INVITACIONES", 
    "BASE IMP. 16 %", 
    "CUOTA 16 %", 
    "BASE IMP. 0 %", 
    "CUOTA 0 %", 
    "TOTAL BASES", 
    "TOTAL IMPUESTOS", 
    "TOTAL"
]


# datatypes dictionary
column_types_dict = {
    'VENTAS': 'float64',             
    'DEVOLUCIONES': 'float64',      
    'TOTAL': 'float64',               
    'VENTAS METALICO': 'float64',     
    'TARJETA DE DEBITO': 'float64', 
    'TARJETA DE CREDITO': 'float64',
    'UBER EATS': 'float64',         
    'RAPPI': 'float64',
    'TRANSFERENCIA': 'float64',             
    'NUM.TICKETS': 'int16',           
    'MEDIA X TICKET': 'float64',      
    'NUMERO DEVOLUCIONES': 'int16',   
    'IMPORTE DEVOLUCIONES': 'float64',
    'NUMERO INVITACIONES': 'int16',   
    'IMPORTE INVITACIONES': 'float64',
    'BASE IMP. 16 %': 'float64',      
    'CUOTA 16 %': 'float64',          
    'BASE IMP. 0 %': 'float64',       
    'CUOTA 0 %': 'float64',           
    'TOTAL BASES': 'float64',         
    'TOTAL IMPUESTOS': 'float64',     
}


new_columns = [
    "sales_date",
    "sales_total", 
    "devolutions", 
    "sales_cash", 
    "sales_debit", 
    "sales_credit", 
    "sales_uber", 
    "sales_rappi", 
    "sales_transfer", 
    "num_tickets", 
    "avg_tickets", 
    "num_devolutions", 
    "total_devolutions", 
    "num_invitations", 
    "total_invitations", 
    "base_tax_16", 
    "amount_tax_16", 
    "base_tax_0", 
    "amount_tax_0", 
    "total_bases", 
    "total_taxes", 
    "total",
    "total_cards"
]


def extract_xlsx_data(directory: str, file_name: str) -> pd.DataFrame:
    """
    Extracting sales data using pandas dataframe

    Args:
        directory (str): file pathe where raw sales data is located
        file_name (str): name of the raw sales file
    Returns:
        raw pd.DataFrame
    """
    return pd.read_excel(
        f"{directory}/{file_name}", 
        sheet_name="venta_mensual", 
        skiprows=2, 
        header=None
    )


def transpose_sales_data(raw_data: pd.DataFrame) -> pd.DataFrame:
    """
    Rearrange data by transposing dataframe

    Args:
        raw_data (pd.DataFrame): Dataframe with raw data sales
    Returns:
        transposed pd.DataFrame
    """
    return (
        raw_data
            .drop(columns=[1])
            .T
            .iloc[:, :-8]
    )


def clean_sales_data(transposed_data: pd.DataFrame) -> pd.DataFrame:
    """
    Removing characters and leading spaces from dataframe string columns

    Args:
        transposed_data (pd.DataFrame)
    Returns:
        cleaned pd.DataFrame
    """
    transposed_data = (
        transposed_data
            .replace("Á", "A", regex=True)
            .replace("- ", "", regex=True)
            .replace("\+ ", "", regex=True)
            .replace(",", "", regex=True)
    )

    transposed_data.columns = transposed_data.iloc[0]
    transposed_data_w_headers = transposed_data[1:]
    return transposed_data_w_headers


def transform_sales_data(transposed_data: pd.DataFrame, columns_to_drop: List[str], columns_ordered: List[str]) -> pd.DataFrame:
    """
    transform data by:
    - adding column 'TRANSFERENCIA' if not present for consistency
    - dropping columns not needed
    - reordering columns

    Args:
        transposed_data (pd.DataFrame):
        columns_to_drop (list):
        columns_ordered (list):
    Returns:
        transformed pd.DataFrame
    """
    # adding "transferencia" column if not present
    if not "TRANSFERENCIA" in list(transposed_data.columns):
        transposed_data["TRANSFERENCIA"] = 0
    
    # drop columns not needed
    transposed_data = (
        transposed_data
        .drop(columns=columns_to_drop)
        .fillna(0)
    )

    # reorder columns
    ordered_data = transposed_data[columns_ordered]
    return ordered_data


def cast_sales_data(cleaned_data: pd.DataFrame, column_types: Dict[str, str]) -> pd.DataFrame:
    """
    Cast columns as datetime, float, or integer accordingly

    Args:
        cleaned_data (pd.DataFrame):
        column_types (dict):
    Returns:
        casted pd.DataFrame
    """
    # convert first column into datetime datatype
    cleaned_data["DIA"] = pd.to_datetime(cleaned_data["DIA"], format="%d/%m/%Y")
    cleaned_data["DIA"] = cleaned_data["DIA"].dt.strftime("%Y-%m-%d")

    # define new data types
    return (
        cleaned_data
            .astype(column_types)
    )


def add_total_card_column(transformed_data: pd.DataFrame) -> pd.DataFrame:
    """
    Adding a new column to summarize card columns

    Args:
        transformed_data (pd.DataFrame):
    Returns:
        grouped pd.DataFrame
    """
    transformed_data["TOTAL TARJETAS"] = transformed_data["TARJETA DE CREDITO"] + transformed_data["TARJETA DE DEBITO"]
    return transformed_data


def rename_columns(data: pd.DataFrame, column_names: List[str]) -> pd.DataFrame:
    """
    Change the name of columns to remove special characters

    Args:
        data (pd.DataFrame): dataframe to be modified the column names
        column_names (List): list of new column names
    Returns:
        dataframe with new column names
    """
    data.columns = column_names
    return data


def generate_sales_report(totals_data: pd.DataFrame) -> str:
    """
    Generate summarize report of sales

    Args:
        totals_data (pd.DataFrame):
    Returns:
        report in string format
    """
    total_sales = totals_data["sales_total"].sum().item()
    total_cash = totals_data["sales_cash"].sum().item()
    total_card = totals_data["total_cards"].sum().item()
    total_uber = totals_data["sales_uber"].sum().item()
    total_rappi = totals_data["sales_rappi"].sum().item()
    total_transfer = totals_data["sales_transfer"].sum().item()

    total_base = totals_data["total_bases"].sum().item()
    total_base_16 = totals_data["base_tax_16"].sum().item()
    total_base_0 = totals_data["base_tax_0"].sum().item()

    total_devolutions = totals_data["total_devolutions"].sum().item()
    total_number_devolutions = totals_data["num_devolutions"].sum().item()

    total_invitations = totals_data["total_invitations"].sum().item()
    total_number_invitations = totals_data["num_invitations"].sum().item()

    total_tax = totals_data["amount_tax_16"].sum().item()

    average_tickets = totals_data["num_tickets"].mean().item()
    total_tickets = totals_data["num_tickets"].sum().item()

    return f"""
    total de efectivo: \t\t$ {total_cash:,.2f}\t+
    total de tarjetas: \t\t$ {total_card:,.2f}\t+
    total de uber: \t\t$ {total_uber:,.2f}\t+
    total de rappi: \t\t$ {total_rappi:,.2f}\t+
    total de transferencias: \t$ {total_transfer:,.2f}
    _______________________________________________
    total ventas: \t\t$ {(total_cash + total_card + total_uber + total_rappi + total_transfer):,.2f}

    total base impuesto 16%: \t$ {total_base_16:,.2f}\t+
    total base impuesto 0%: \t$ {total_base_0:,.2f}
    _______________________________________________
    total base: \t\t$ {total_base:,.2f}\t+

    cuota iva 16%: \t\t$ {total_tax:,.2f}
    _______________________________________________
    total ventas: \t\t$ {total_sales:,.2f}


    numero de devoluciones: \t{total_number_devolutions:,}
    valor de devoluciones: \t$ {abs(total_devolutions):,.2f}

    numero de invitaciones: \t{total_number_invitations:,}
    valor de invitaciones: \t$ {total_invitations:,.2f}

    total tickets: \t\t{total_tickets:,}
    promedio por ticket: \t$ {average_tickets:,.2f}
    """


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


def create_raw_tables(conn: object) -> None:
    """
    """
    print("Creating raw table if not exists ...\n")
    try:
        cursor = conn.cursor()
        cursor.execute(f"""
            CREATE SCHEMA IF NOT EXISTS {ENV};
            
            CREATE TABLE IF NOT EXISTS {ENV}.raw_sales_data (
                id_sales SERIAL PRIMARY KEY,
                sales_date DATE,
                sales_total NUMERIC,
                devolutions NUMERIC,
                sales_cash NUMERIC,
                sales_credit NUMERIC,
                sales_debit NUMERIC,
                sales_uber NUMERIC,
                sales_rappi NUMERIC,
                sales_transfer NUMERIC,
                num_tickets INTEGER,
                avg_tickets NUMERIC,
                num_devolutions INTEGER,
                total_devolutions NUMERIC,
                num_invitations INTEGER,
                total_invitations NUMERIC,
                base_tax_16 NUMERIC,
                amount_tax_16 NUMERIC,
                base_tax_0 NUMERIC,
                amount_tax_0 NUMERIC,
                total_bases NUMERIC,
                total_taxes NUMERIC,
                total NUMERIC,
                total_cards NUMERIC,
                date_insertion TIMESTAMP DEFAULT NOW()
            );
        """)
        conn.commit()
        print("Raw tables created successfully!\n")
    except psycopg2.Error as e:
        print(f"Failed to create stage tables: {e}")
        raise


def load_sales_data(conn: object, data: pd.DataFrame) -> None:
    """
    Inserts a pandas DataFrame into a PostgreSQL table using psycopg2

    Args:
        conn (object): object database connection
        data (pd.DataFrame): DataFrame to insert
    
    Returns:
        None
    """
    try:
        # establish connection
        cursor = conn.cursor()
        
        # prepare data: convert Dateframe to list of tuples
        columns = list(data.columns)
        print(columns, end="\n\n")
        values = [tuple(row) for row in data.to_numpy()]

        # create SQL query
        columns_str = ', '.join(columns)
        print(columns_str)
        query = f"INSERT INTO {ENV}.raw_sales_data ({columns_str}) VALUES %s"

        # execute bulk insert
        execute_values(cursor, query, values)

        # commit transaction
        conn.commit()

    except psycopg2.Error as e:
        print(f"Error inserting data into database: {e}")
    except Exception as e:
        print(f"Failed to insert records: {e}")
        raise


def main():

    # defining paths of directories for invoices and vouchers
    sales_dir = f"{HOME_DIRECTORY}/{SALES_XLSX_DIRECTORY}"
    # print(sales_dir, end="\n\n")

    # getting list of invoices and vouchers
    xlsx_list = get_files(sales_dir)
    # print(xlsx_list, end="\n\n")

    # extracting sales data
    raw_data = extract_xlsx_data(sales_dir, xlsx_list[0])
    # print(raw_data.head())

    # transposing sales data
    transposed_data = transpose_sales_data(raw_data)
    # print(transposed_data.head())

    # cleaning string data
    cleaned_data = clean_sales_data(transposed_data)
    # print(cleaned_data.head())

    # transforming sales data
    transformed_data = transform_sales_data(cleaned_data, columns_to_drop, columns_ordered)
    # print(transformed_data.head())

    # casting data types
    casted_data = cast_sales_data(transformed_data, column_types_dict)
    # print(casted_data.head())

    # generating a total column for credit and debit sales
    totals_data = add_total_card_column(casted_data)
    # print(totals_data.columns)

    # changing column names to remove special characters
    sales_data = rename_columns(totals_data, new_columns)
    # print(sales_data.head(), end="\n\n")

    conn = connect_to_db()
    create_raw_tables(conn)

    try:
        # inserting sales data
        load_sales_data(conn, sales_data)
        print("\nSales data inserted successfully!\n")
        
    except Exception as e:
        print(f"An error occurred during execution: {e}")
    finally:
        if 'conn' in locals():
            conn.close()
            print("Database connection closed.\n")


    # generating summary sales report
    print(generate_sales_report(sales_data))


# main()