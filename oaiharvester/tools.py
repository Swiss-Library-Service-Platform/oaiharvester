from datetime import date, timedelta
import logging
import sys
import shutil
import os
from typing import Optional, List
import re

# xml namespaces
ns = {'oai': 'http://www.openarchives.org/OAI/2.0/',
      'marc': 'http://www.loc.gov/MARC21/slim'}


def configure_logger(job_name: str, set_name: str) -> None:
    """
    Configure the logger for the application
    """
    # Close previous handlers
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    # Log configuration
    message_format = "%(asctime)s - %(levelname)s - %(message)s"
    if not os.path.exists('log'):
        os.mkdir('log')
    log_file_name = f'log/log_{job_name}_{set_name}_{date.today().isoformat()}.txt'
    logging.basicConfig(format=message_format,
                        level=logging.INFO,
                        handlers=[logging.FileHandler(log_file_name),
                                  logging.StreamHandler(sys.stdout)],
                        force=True)


def check_free_space(path: str, low_limit: Optional[int] = 25, error_limit: Optional[int] = 10) -> bool:
    """
    Check if there is enough free space to save the file

    10 GB minimum must be free

    Parameters
    ----------
    path : str
        Path to check
    low_limit : int, optional
        Minimum free space in GB, default is 25
    error_limit : int, optional
        Minimum free space in GB to raise an error, default is 10

    """
    disk = shutil.disk_usage(path)
    if error_limit * 1024**3 < disk.free < low_limit * 1024**3:
        logging.warning(f'Low disk space: {round(disk.free / 1024**3, 3)} GB')

    if disk.free < error_limit * 1024**3:
        logging.error(f'Not enough disk space: {round(disk.free / 1024**3, 3)} GB')
        return False
    return True


def get_newest_chunks_list() -> List[str]:
    """
    Get a list of the newest chunks in the directory

    Returns
    -------
    list
        List of the newest chunk paths
    """
    directories = os.listdir('harvested_data')
    directories = [directory for directory in directories
                   if re.match(r'^OaiSet_\w+_\d{8}$', directory)]
    if len(directories) == 0:
        return []

    directories.sort(reverse=True)

    newest_directory = directories[0]

    harvesting_date = get_date_from_chunk_directory(newest_directory)

    if harvesting_date + timedelta(days=4) < date.today():
        logging.critical(f'No new chunks found in {newest_directory} - last harvesting: {harvesting_date}')
        return []

    chunk_names = os.listdir(f'harvested_data/{newest_directory}')
    chunk_paths = [f'harvested_data/{newest_directory}/{f}' for f in chunk_names
                   if re.match(r'^chunk_\w+_\d{5}.xml$', f)]
    chunk_paths.sort()

    return list(chunk_paths)


def get_date_from_chunk_directory(directory: str) -> date:
    """
    Get the date from the chunk directory

    Parameters
    ----------
    directory : str
        Directory path

    Returns
    -------
    date
        Date from the directory
    """
    m = re.match(r'^OaiSet_\w+_(\d{4})(\d{2})(\d{2})$', os.path.basename(directory))
    return date(year=int(m.group(1)), month=int(m.group(2)), day=int(m.group(3)))


def get_chunk_list_from_directory(directory: str) -> List[str]:
    """
    Get a list of chunks in the directory

    Parameters
    ----------
    directory : str
        Directory path

    Returns
    -------
    list
        List of chunk paths
    """
    chunk_names = os.listdir(directory)
    chunk_paths = [f'{directory}/{f}' for f in chunk_names if re.match(r'^chunk_\w+_\d{5}.xml$', f)]
    chunk_paths.sort()
    return list(chunk_paths)


def get_directory_param() -> Optional[str]:
    """
    Get the directory parameter from the command line

    Returns
    -------
    str
        Directory path or None if no parameter is given from command line
    """
    if len(sys.argv) == 3 and sys.argv[1] == '--directory':
        directory = os.path.normpath(sys.argv[2])
        if not os.path.isdir(directory):
            logging.critical(f'Directory {directory} does not exist')
            sys.exit(1)
        return directory
