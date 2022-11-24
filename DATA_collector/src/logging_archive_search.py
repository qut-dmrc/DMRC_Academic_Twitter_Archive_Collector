import logging
from datetime import datetime
import sys


def set_up_logging(logfile_filepath):
    logtime = str(datetime.now().strftime("%Y%m%d%H%M%S"))

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s",
                        handlers=[logging.FileHandler(f'{logfile_filepath}/archive_collection_{logtime}.log', encoding='utf-8'),
                            logging.StreamHandler(sys.stdout)])
    print('logging.basicConfig set')
    print(f'Log file located at: {logfile_filepath}/archive_collection_{logtime}.log')


