'''
This file contains functions for setting up the directory environment for the archive search.
'''

import os.path
import glob
from time import sleep

from .logging_archive_search import *
from .config import *


# Get current working directory
cwd = str(os.getcwd()).replace('\\', '/')

# Get search parameters from config.py
dataset = GBQ.dataset
folder = dataset
new_dir = folder + '/'
dir_name = str(f'{cwd}/my_collections/{new_dir}')

# Specify file paths for outputs and temp csv files
json_filepath = "collected_json/"
json_filepath = str(dir_name + json_filepath)
csv_filepath = 'generated_csv/'
csv_filepath = str(dir_name + csv_filepath)
error_filepath = 'error_logs/'
error_filepath = str(dir_name + error_filepath)
logfile_filepath = f'{cwd}/logging'


def set_up_directories(logfile_filepath, dir_name, folder, json_filepath, csv_filepath, error_filepath):
    '''
    Call functions to set directories and file paths
    '''

    set_up_logging(logfile_filepath, folder)
    sleep(0.5)
    set_directory(dir_name, folder)
    sleep(0.5)
    set_json_path(json_filepath, folder)
    sleep(0.5)
    set_csv_path(csv_filepath, folder)
    sleep(0.5)
    set_error_log_path(error_filepath, folder)
    sleep(0.5)
    set_log_file_path(logfile_filepath, folder)
    sleep(0.5)

def set_directory(dir_name, folder):
    '''
    Create a new main directory for the archive search, based on dataset name
    '''

    logging.info('Checking for existing directory...')
    try:
        os.mkdir(dir_name)
        logging.info('Path does not yet exist')
        logging.info(f"Created new file directory named {folder} at location {dir_name}")
    except OSError:
        logging.info(f"{folder} already exists at location {dir_name}")
        logging.info(f"files will be written to this existing location")
    logging.info('-----------------------------------------------------------------------------------------')

def set_json_path(json_filepath, folder):
    '''
    Create folder for collected, raw json files to be saved to
    '''

    logging.info('Checking for JSON filepath...')
    try:
        os.mkdir(json_filepath)
        logging.info('Path does not yet exist')
        logging.info(f"Created new JSON file directory named {folder} at location {json_filepath}")
    except OSError:
        logging.info(f"JSON file directory already exists at location {json_filepath}")
        logging.info(f"json files will be written to this existing location")
    logging.info('-----------------------------------------------------------------------------------------')

def set_csv_path(csv_filepath, folder):
    '''
    Create folder for processed csv files to be saved to
    '''

    logging.info('Checking for CSV filepath...')
    try:
        os.mkdir(csv_filepath)
        logging.info('Path does not yet exist')
        logging.info(f"Created new CSV file directory named {folder} at location {csv_filepath}")
    except OSError:
        logging.info(f"CSV file directory already exists at location {csv_filepath}")
        logging.info(f"csv files will be written to this existing location")
    logging.info('-----------------------------------------------------------------------------------------')

def set_error_log_path(error_filepath, folder):
    '''
    Create folder for error text files to be saved to
    '''

    logging.info('Checking for error log filepath...')
    try:
        os.mkdir(error_filepath)
        logging.info('Path does not yet exist')
        logging.info(f"Created new error file directory named {folder} at location {error_filepath}")
    except OSError:
        logging.info(f"error log file directory already exists at location {error_filepath}")
        logging.info(f"error log files will be written to this existing location")
    logging.info('-----------------------------------------------------------------------------------------')

def set_log_file_path(logfile_filepath, folder):
    '''
    Creates folder for log files to be saved to
    '''

    logging.info('Checking for logging filepath...')
    try:
        os.mkdir(logfile_filepath)
        logging.info('Path does not yet exist')
        logging.info(f"Created new log file directory named {folder} at location {logfile_filepath}")
    except OSError:
        logging.info(f"log file directory already exists at location {logfile_filepath}")
        logging.info(f"log files will be written to this existing location")
    logging.info('-----------------------------------------------------------------------------------------')

def set_up_expected_files(start_date, end_date, json_filepath, option_selection, query, dataset, interval, query_count):
    '''
    Divides search into multiple queries, based on the interval chosen in config.yml.
    This saves on memory by 'chunking' long and voluminous searches into separate collections based on number of days
    (n_intervals). Interval parameter can be any number. 0.25 = 6 hours. 0.5 = 12 hours. 1 =  24 hours. 90 = ~3 months,
    and so on. Interval is generated automatically in data.calculate_interval().
    '''

    if query_count == None:
        query_count = ''
    elif query_count == 1:
        query_count = ''

    window_length = dt.timedelta(days=interval)
    expected_files = dict()
    current_date = start_date
    saved_search_path = json_filepath

    if option_selection == 'lv':
        print(option_selection)
        dataset = query
    # Generate dictionary of file names and start and end dates
    # TODO make last filename end date correct
    while current_date < end_date:
        expected_files[
            saved_search_path + f"{dataset}{query_count}_{current_date.isoformat()}_{(current_date+window_length).isoformat()}_tweets.jsonl".replace(":", "").replace(" ", "")] = (
            current_date,
            current_date + window_length
        )
        current_date += window_length

    # for item in expected_files:

    # TODO make this nicer - this is very makeshift.
    #  The idea is to change the last end date in the dict to the end date in the config.
    #  It is based on the window length so usually the end date is extended to the next interval.
    kvps = list(expected_files.items())
    kvps_list = list(kvps[-1])
    kvps_list_list = list(kvps_list[-1])
    kvps_list_list[-1] = Query.end_date
    kvps_list_list = tuple(kvps_list_list)
    kvps_list[-1] = kvps_list_list
    kvps[-1] = tuple(kvps_list)

    expected_files = dict(kvps)
    collected_files = glob.glob(saved_search_path + "*jsonl")
    collected_files = set([filename.replace('\\', '/') for filename in collected_files])
    to_collect = set(expected_files) - collected_files
    to_collect = sorted(to_collect)
    not_to_collect = result = [i for i in expected_files if i in collected_files]
    return to_collect, not_to_collect, expected_files

def get_json_input_files():
    '''
    Sets the location for json input files
    '''
    json_input_filepath = f'{cwd}/json_input_files/'
    json_input_files = glob.glob(json_input_filepath + "*jsonl")
    # json_input_filepath = '//rstore.qut.edu.au/projects/cif/auspubsphere/dmrc_DATA_collection/x_journalism/collected_json/'
    # json_input_files = glob.glob(json_input_filepath + "*jsonl")
    # json_input_filepath = 'C:/Users/vodden/PycharmProjects/AWS_boto/pulled_files/done/'
    # json_input_files = glob.glob(json_input_filepath + "*jsonl")

    return json_input_files

