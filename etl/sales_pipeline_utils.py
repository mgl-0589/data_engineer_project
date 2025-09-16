import os
from dotenv import load_dotenv
from file_manager import get_year, get_files
import pandas as pd
from typing import Dict, List
import psycopg2
import pandas as pd
import numpy as np


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
            .set_index("DIA")
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


def generate_sales_report(totals_data: pd.DataFrame) -> str:
    """
    Generate summarize report of sales

    Args:
        totals_data (pd.DataFrame):
    Returns:
        report in string format
    """
    total_sales = totals_data["TOTAL"].sum().item()
    total_cash = totals_data["VENTAS METALICO"].sum().item()
    total_card = totals_data["TOTAL TARJETAS"].sum().item()
    total_uber = totals_data["UBER EATS"].sum().item()
    total_rappi = totals_data["RAPPI"].sum().item()
    total_transfer = totals_data["TRANSFERENCIA"].sum().item()

    total_base = totals_data["TOTAL BASES"].sum().item()
    total_base_16 = totals_data["BASE IMP. 16 %"].sum().item()
    total_base_0 = totals_data["BASE IMP. 0 %"].sum().item()

    total_devolutions = totals_data["DEVOLUCIONES"].sum().item()
    total_number_devolutions = totals_data["NUMERO DEVOLUCIONES"].sum().item()

    total_invitations = totals_data["IMPORTE INVITACIONES"].sum().item()
    total_number_invitations = totals_data["NUMERO INVITACIONES"].sum().item()

    total_tax = totals_data["CUOTA 16 %"].sum().item()

    average_tickets = totals_data["NUM.TICKETS"].mean().item()
    total_tickets = totals_data["NUM.TICKETS"].sum().item()

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




def main():

    # # get year
    # year = get_year(year)
    # # print(year, end="\n\n")

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
    # print(totals_data.head())

    # generating summary sales report
    print(generate_sales_report(totals_data))


# main()