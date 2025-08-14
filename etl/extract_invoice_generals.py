import os
import shutil
import sys
import xml.etree.ElementTree as ET
from path_generator import PathGenerator

# define global variables
XML_SUFFIX = ".xml"
PDF_SUFFIX = ".pdf"
NS = {"cfdi": "http://www.sat.gob.mx/cfd/4"}

# define "year" as argument provided when execution of script
YEAR = sys.argv[1]


def create_directories(directory: str) -> None:
    """
    Create directory if not exist
    
    Args:
        directory (str): directory path to be created if not exist
    """
    if not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)


def get_files_list(source_directory: str) -> list[str]:
    """ 
    Get the list of invoice names that has not been modified (if not starting with 'year') 
    
    Args:
        source_directory (str): path of source directory where invoice file is located
    """
    invoices_list = os.listdir(source_directory)
    return invoices_list


def validate_file_name(invoice_name: str) -> str:
    """
    Valdiate if the name has more than one '.' character, and remove all extra dots

    Args:
        invoice_name (str): name of the invoice file
    """
    if invoice_name.count(".") > 1:
        return ".".join([invoice_name.rsplit(".", 1)[0].replace(".", "_"), invoice_name.rsplit(".", 1)[1]])
    else:
        return invoice_name


def change_file_name(source_directory: str, old_name: str, new_name: str) -> None:
    """
    Change the name of a file

    Args:
        source_directory (str): path of source directory where invoice file is located
        old_name (str): old name before extra-dot-validation
        new_name (str): new name post extra-dot_validation
    """
    old_name = f"{source_directory}/{old_name}"
    new_name = f"{source_directory}/{new_name}"
    os.rename(old_name, new_name)


def extract_invoice_generals(source_directory: str, file_name: str) -> dict[str: str]:
    """
    Extract the general information from each xml file
    
    Args:
        source_path (str): source directory path from where files will be extracted
        file_name (str): current name of file that will be read
    """
    try:
        xml_tree = ET.parse(f"{source_directory}/{file_name}")
        root = xml_tree.getroot()
    except FileNotFoundError as e:
        print(f"File not found error: {e}")
        
    # getting general invoice data
    generals = root.attrib
    date = generals["Fecha"]
    currency = generals["Moneda"]
    transaction_type = generals["TipoDeComprobante"]
    subtotal = generals["SubTotal"]
    total = generals["Total"]
    try:
        invoice_id = generals["Folio"]
    except KeyError:
        invoice_id = f"SF-{date.replace("-", "")}"
    
    # getting transmitter data
    generals_transmitter = root.find("cfdi:Emisor", NS)
    transmitter_id = generals_transmitter.attrib["Rfc"]
    transmitter_name = generals_transmitter.attrib["Nombre"]

    # getting receiver data
    generals_receiver = root.find("cfdi:Receptor", NS)
    receiver_id = generals_receiver.attrib["Rfc"]
    # receiver_name = generals_receiver.attrib["Nombre"]

    # return dictionary with general information
    return {
        "source_xml_name": f"{file_name}",
        "source_pdf_name": f"{file_name.split(XML_SUFFIX)[0]}{PDF_SUFFIX}",
        "new_base_name": f"{date.split("T")[0]}_{transmitter_id}_{invoice_id}_{transaction_type}",
        "date": date,
        "invoice_id": invoice_id,
        "currency": currency,
        "transaction": transaction_type,
        "subtotal": subtotal,
        "total": total,
        "transmitter_id": transmitter_id,
        "transmitter_name": transmitter_name,
        "receiver_id": receiver_id 
    }


def copy_files(source_path: str, target_path: str, old_name: str, new_name: str, suffix: str) -> None:
    """
    Copy .xml and .pdf files to respective target folder (invoice/ | voucher/)
    
    Args:
        source_path (str): source directory path from where files will be extracted
        target_path (str): target directory path to where files will be copied
        old_name (str): current name of source file
        new_name (str): new name of file
        suffix (str): suffix indicating type of file (.xml | .pdf)
    """
    # print(f"copying file from {source_path} into {target_path} ...")
    try:
        shutil.copy(f"{source_path}/{old_name}", f"{target_path}/{new_name}{suffix}")
    except FileNotFoundError as e:
        print(f"File not found, error: {e}")
    
    """ copy pdf files to respective folder ..."""
    """ create first commit with git ..."""


########################### testing ###########################
print(f"\n{sys.executable}\n")


# create folders if not exist
directory = PathGenerator(YEAR)
# print(directory.source_path)
# print(directory.target_xml_path)
# print(directory.target_pdf_path)

list_directories = [
                    directory.source_path, 
                    directory.target_invoice_xml_path, 
                    directory.target_invoice_pdf_path, 
                    directory.target_voucher_xml_path, 
                    directory.target_voucher_pdf_path
                   ]


# creation of directories recursively
print(f"creation of source and target directories started ...\n")

for directory_name in list_directories:
    try:
        create_directories(directory_name)
    except FileNotFoundError as e:
        print(f"Directory not found: {e}")

print(f"source and target directories created successfully!\n")


# extract the file names in source directory
original_names_list = get_files_list(directory.source_path)
# print(f"{original_names_list}\n")


# remove "." characters if file name has more than 1
validated_names_list = [*map(validate_file_name, original_names_list)]
# print(f"{validated_names_list}\n")


# combine the original and modified names in a list of tuples
names_combined_list = [(old_name, new_name) for old_name, new_name in zip(original_names_list, validated_names_list) if old_name != new_name]
# print(f"{names_combined_list}\n")


# change the name of files that has been validated to remove extra "." characters
for old_name, new_name in names_combined_list:
    # change file name if old_name and new name are not equal due to extra-dot-validation
    if old_name != new_name:
        try:
            # print(f"old_name: {old_name}, new_name: {new_name}")
            change_file_name(directory.source_path, old_name, new_name)
        except FileExistsError as e:
            print(f"File not found: {e}")

invoice_names_list = get_files_list(directory.source_path)
# print(invoice_names_list)


# removing pdf files from list
xml_invoice_list = [invoice for invoice in invoice_names_list if invoice.endswith(XML_SUFFIX)]


# extract the general information from each xml invoice file
print("\nExtracting general data from invoices ...\n")

invoice_generals_list = [extract_invoice_generals(directory.source_path, invoice) for invoice in xml_invoice_list]
# print(invoice_generals_list)
# print(f"\nsource xml name: {invoice_generals_list[0]["source_xml_name"]} \nsource pdf name: {invoice_generals_list[0]["source_pdf_name"]} \nnew base name: {invoice_generals_list[0]["new_base_name"]}")

print("\nGenerals data extracted successfully!\n")


# copy files to respective target folder
print(f"\nCopying files from source directory to respective target directory ...\n")
for file in invoice_generals_list:
    
    if file["new_base_name"].endswith("_I"):
        # print(f"{directory.source_path}/{file["source_xml_name"]}") 
        # print(f"{directory.source_path}/{file["source_pdf_name"]}")
        # print(f"{xml_target}/{file["new_base_name"]}{XML_SUFFIX}") 
        # print(f"{pdf_target}/{file["new_base_name"]}{PDF_SUFFIX}")

        xml_target = directory.target_invoice_xml_path
        pdf_target = directory.target_invoice_pdf_path
        
        copy_files(directory.source_path, xml_target, file["source_xml_name"], file["new_base_name"], XML_SUFFIX)
        copy_files(directory.source_path, pdf_target, file["source_pdf_name"], file["new_base_name"], PDF_SUFFIX)

    
    elif file["new_base_name"].endswith("_P"):
        # print(f"{directory.source_path}/{file["source_xml_name"]}") 
        # print(f"{directory.source_path}/{file["source_pdf_name"]}")
        # print(f"{xml_target}/{file["new_base_name"]}{XML_SUFFIX}") 
        # print(f"{pdf_target}/{file["new_base_name"]}{PDF_SUFFIX}")

        xml_target = directory.target_voucher_xml_path
        pdf_target = directory.target_voucher_pdf_path

        copy_files(directory.source_path, xml_target, file["source_xml_name"], file["new_base_name"], XML_SUFFIX)
        copy_files(directory.source_path, pdf_target, file["source_pdf_name"], file["new_base_name"], PDF_SUFFIX)
        
print(f"\nAll files copied successfully!\n")
 

