# import os
import psycopg2
import sys
from datetime import datetime
import xml.etree.ElementTree as ET
from path_generator import PathDefiner
from extract_invoice_generals import extract_invoice_summary, get_files_list, extract_year, NS


#################### functions ####################

def connect_to_db():
    print("Connecting to the PostgreSQL database ...")
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5000,
            dbname="coffee_shop",
            user="coffee_shop_user",
            password="coffee_shop_password"
        )
        return conn
    except psycopg2.Error as e:
        print(f"Database connection failed: {e}")
        raise


def create_table(conn):
    print("Creating table if not exists ...\n")
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS dev;
            CREATE TABLE IF NOT EXISTS dev.stg_summary_invoices (
                id_stg_general_invoice SERIAL PRIMARY KEY,
                file_name TEXT,
                date_creation TEXT,
                invoice_id TEXT,
                currency VARCHAR(5),
                subtotal FLOAT,
                total FLOAT,
                transmitter_id TEXT,
                transmitter_name TEXT,
                receiver_id TEXT,
                receiver_name TEXT,
                date_insertion TIMESTAMP DEFAULT NOW()
            );
        """)
        conn.commit()
        print("Table created successfully!\n")
    except psycopg2.Error as e:
        print(f"Failed to create table: {e}")
        raise


def insert_records(conn, data):
    print("Inserting invoice summary data into database ...")
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO dev.stg_summary_invoices (
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
                date_insertion
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        """, (
            data["source_xml_name"],
            data["date"],
            data["invoice_id"],
            data["currency"],
            float(data["subtotal"]),
            float(data["total"]),
            data["transmitter_id"],
            data["transmitter_name"],
            data["receiver_id"],
            data["receiver_name"]
        ))
        conn.commit()
        print("Data inserted successfully!\n")
    except psycopg2.Error as e:
        print(f"Error inserting data into the database: {e}")
        raise
    

#################### functions ####################

def main():
    try:
        # show executable script 
        print(f"\nExecutable: {sys.executable}\n")

        # extract year
        year = extract_year()

        # create folders if not exist
        directory = PathDefiner(year)
        invoices_directory = directory.target_invoice_xml_path

        # extract xml invoices
        invoices_list = get_files_list(invoices_directory)

        invoice_generals_list = [extract_invoice_summary(invoices_directory, invoice, NS) for invoice in invoices_list]

        # connection to db
        conn = connect_to_db()

        # create schema and table
        create_table(conn)

        # insert invoice summary data
        for invoice in invoice_generals_list:
            insert_records(conn, invoice)
    except Exception as e:
        print(f"An error occurred during execution: {e}")
    finally:
        if 'conn' in locals():
            conn.close()
            print("Database connection closed.\n")


main()

