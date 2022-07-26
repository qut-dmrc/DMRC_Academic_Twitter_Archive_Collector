# This file contains functions for setting up the directory environment for the archive search
import os.path
import glob


from .logging_archive_search import *
from .config import *

# get current working directory
# -----------------------------
cwd = str(os.getcwd()).replace('\\', '/')

dataset = 'rebelwilson'
dataset = GBQ.dataset
folder = dataset
new_dir = folder + '/'
dir_name = str(f'{cwd}/{new_dir}')

json_filepath = "collected_json/"
json_filepath = str(dir_name + json_filepath)
csv_filepath = 'generated_csv/'
csv_filepath = str(dir_name + csv_filepath)
error_filepath = 'error_logs/'
error_filepath = str(dir_name + error_filepath)
logfile_filepath = f'{cwd}/logging'

# Create a new directory for the archive search
# ---------------------------------------------
def set_directory(dir_name, folder):
    logging.info('Checking for existing directory...')
    try:
        os.mkdir(dir_name)
        logging.info('Path does not yet exist')
        logging.info(f"Created new file directory named {folder} at location {dir_name}")
    except OSError as error:
        logging.info(f"{folder} already exists at location {dir_name}")
        logging.info(f"files will be written to this existing location")
    logging.info('-----------------------------------------------------------------------------------------')


# Create folder for collected, raw json files to be saved to
# ----------------------------------------------------------
def set_json_path(json_filepath, folder):
    logging.info('Checking for JSON filepath...')
    try:
        os.mkdir(json_filepath)
        logging.info('Path does not yet exist')
        logging.info(f"Created new JSON file directory named {folder} at location {json_filepath}")
    except OSError as error:
        logging.info(f"JSON file directory already exists at location {json_filepath}")
        logging.info(f"json files will be written to this existing location")
    logging.info('-----------------------------------------------------------------------------------------')


# Create folder for processed csv files to be saved to
# ----------------------------------------------------
def set_csv_path(csv_filepath, folder):
    logging.info('Checking for CSV filepath...')
    try:
        os.mkdir(csv_filepath)
        logging.info('Path does not yet exist')
        logging.info(f"Created new CSV file directory names {folder} at location {csv_filepath}")
    except OSError as error:
        logging.info(f"CSV file directory already exists at location {csv_filepath}")
        logging.info(f"csv files will be written to this existing location")
    logging.info('-----------------------------------------------------------------------------------------')


# Create folder for error text files to be saved to
# -------------------------------------------------
def set_error_log_path(error_filepath, folder):
    logging.info('Checking for error log filepath...')
    try:
        os.mkdir(error_filepath)
        logging.info('Path does not yet exist')
        logging.info(f"Created new error file directory named {folder} at location {error_filepath}")
    except OSError as error:
        logging.info(f"error log file directory already exists at location {error_filepath}")
        logging.info(f"error log files will be written to this existing location")
    logging.info('-----------------------------------------------------------------------------------------')


# Create folder for log files to be saved to
# ------------------------------------------
def set_log_file_path(logfile_filepath, folder):
    logging.info('Checking for logging filepath...')
    try:
        os.mkdir(logfile_filepath)
        logging.info('Path does not yet exist')
        logging.info(f"Created new log file directory named {folder} at location {logfile_filepath}")
    except OSError as error:
        logging.info(f"log file directory already exists at location {logfile_filepath}")
        logging.info(f"log files will be written to this existing location")
    logging.info('-----------------------------------------------------------------------------------------')



# Construct a range of datetimes to use to collect Tweets in specified n_day intervals (1 interval = 1 json file)
# ---------------------------------------------------------------------------------------------------------------
def set_up_expected_files(start_date, end_date, json_filepath):
    window_length = dt.timedelta(days=Query.interval_days)
    expected_files = dict()
    current_date = start_date
    saved_search_path = json_filepath

    # Generate dictionary of file names, start date and end date
    while current_date < end_date:
        expected_files[saved_search_path + f"{current_date.isoformat()}_tweets.jsonl".replace(":", "")] = (
            current_date,
            current_date + window_length
        )
        current_date += window_length

    collected_files = glob.glob(saved_search_path + "*jsonl")
    collected_files = set([filename.replace('\\', '/') for filename in collected_files])
    to_collect = set(expected_files) - collected_files
    to_collect = sorted(to_collect)

    return to_collect, expected_files