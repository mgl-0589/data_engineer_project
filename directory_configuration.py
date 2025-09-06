import os
from datetime import datetime
import logging
import sys


# define path of base directories
HOME_DIRECTORY = os.path.expanduser("~")
BASE_DICRECTORY = "repos/coffee_shop_data_project"
SOURCE_DIRECTORY = "source_files"
TARGET_DIRECTORY = "target_files"
LOGS_DIRECTORY = "logs"


# extract year
def define_year(year: str|int =datetime.now().year) -> str:
    """
    Extract the year to specify from which source and target directories data will be extracted / located

    Args:
        year (str): year of interest
    """
    if len(sys.argv) > 1:
        return sys.argv[1]
    return str(year)


# generate path function
def generate_path(*args: str) -> str:
    """
    Create source directory for source files

    Args:
        *args (str): one or more directories to be considered for creating the path
    Returns:
        string path
    """
    print(os.path.join(*args))
    return os.path.join(*args)


# create directory function
def create_directory(directory_name: str) -> None:
    """
    Create the directory specified if not exists

    Args:
        directory_path (str): absolute path of directory to be created
    Returns:
        None
    """
    try:
        if not os.path.exists(directory_name):
            os.makedirs(directory_name, exist_ok=True)
    except OSError as e:
            print(f"Directory not found: {e}")


# setup logging directories to capture errors and information of execution
def setup_logging_directory(directory: str, process_name: str, year: str) -> None:
    """
    Setting up the logs directory
    
    Args:
        directory (str):
        process_name (str):
        year (str):
    Returns:
        None
    """
    logging.basicConfig(
        filename=f"{directory}/{year}/{process_name}{datetime.now().date()}.log",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    ) 



##################################### testing #####################################
def main():

    print(f"\nExecutable: {sys.executable}\n")
    
    # extract year
    year = define_year()


    # generate path directories and create each of them
    print(f"\nCreating directories ...\n")

    directories = {
        'source': [SOURCE_DIRECTORY],
        'target': [TARGET_DIRECTORY],
        'logs': [LOGS_DIRECTORY, year],
        'xml': [TARGET_DIRECTORY, "xml", year],
        'pdf': [TARGET_DIRECTORY, "pdf", year],
    }

    for directory in directories.values():
        path = generate_path(HOME_DIRECTORY, BASE_DICRECTORY, *directory)
        create_directory(path)


    # setting up the logging directories
    # one setup for each process (extract, transform, load) ...


    print(f"\nDirectories created successfully!\n")

main()
