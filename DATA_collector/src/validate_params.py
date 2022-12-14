'''
Contains functions for validating user input
'''

import pandas as pd
import re
import pytz

from .set_up_directories import *

pd.options.mode.chained_assignment = None
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)


class ValidateParams:

    def validate_search_parameters(self, query, bearer_token, start_date, end_date, project, dataset):
        '''
        Validates search parameters when Option 1 (Search Archive) selected.
        Unable to check bearer token validity beyond ensuring it is in config.yml; checks for presence only.
        If parameter is invalid, then parameter=None and program will notify user and exit.
        '''

        utc = pytz.UTC

        print("""
        Validating your config...
        """)

        # Search query length; if str query, check min and max len of query. Else, if list query, check min len.
        if type(query) == str:
            if 'AND' in query:
                print('AND operator is represented by a space between keywords. Please refer to the documentation and edit your query.')
                query = None
            else:
                if len(query) in range(1, 1024):
                    query = query
                elif len(query) < 1:
                    print('Please enter a search query.')
                    query = None
                else:
                    query = None
                    print(
                        f'Query is too long ({len(query)} characters). Please shorten it to 1024 characters or fewer and retry.')
        else:
            if type(query) == str:
                if len(query) < 1:
                    print('Please enter a search query.')
                    query = None

        # Check bearer token entered; only checks len for presence of a bearer token
        if len(bearer_token) > 0:
            bearer_token = bearer_token
        else:
            bearer_token = None
            print('Please enter a valid bearer token for Twitter API access.')

        # Check for Google access key; looks for .json service account key in the 'access_key' dir
        if glob.glob(f'{cwd}/access_key/*.json'):
            access_key = glob.glob(f'{cwd}/access_key/*.json')
        else:
            access_key = None
            print('Please enter a valid Google service account access key')

        # Ensure start date is in the past, but must be before end date
        if start_date < end_date:
            start_date = start_date
        elif start_date > utc.localize(dt.datetime.now()):
            start_date = None
            print('Start date cannot be in the future.')
        else:
            start_date = None
            print('Start date cannot be after end date.')

        # Ensure end date is in the past
        if end_date < utc.localize(dt.datetime.now()):
            end_date = end_date
        else:
            end_date = None
            print('End date cannot be in the future.')

        # Check project name entered; checks len for presence of a project string and checks invalid characters
        if len(project) > 0:
            if bool(re.match("^[A-Za-z0-9-]*$", project)) == True:
                project = project
            else:
                project = None
                print('Invalid project name. Project names may contain letters, numbers, dashes and underscores.')
        else:
            project = None
            print('No project in config.')

        # Check dataset name entered; checks len for presence of a dataset string and checks invalid characters
        if len(dataset) > 0:
            if bool(re.match("^[A-Za-z0-9_]*$", dataset)) == True:
                dataset = dataset
            else:
                print('Invalid dataset name. Dataset names may contain letters, numbers and underscores.')
                dataset = None
        else:
            dataset = None
            print('No dataset in config.')


        # If any of the above parameters are None, exit program; else, proceed.
        if None in [query, bearer_token, access_key, start_date, end_date, project, dataset]:
            print("""
            \n 
        Exiting...
            \n""")
            exit()
        else:
            print("""
            \n
        Config input valid!
            \n""")

            return query, bearer_token, access_key, start_date, end_date, project, dataset

    def validate_project_parameters(self, project, dataset):
        '''
        Validates project parameters when Option 2 (Process from .json) is selected.
        If parameter is invalid, then parameter=None and program will notify user and exit.
        '''

        query = 'JSON upload'
        start_date = ''
        end_date = ''

        # Check for Google access key; looks for .json service account key in the 'access_key' dir
        if glob.glob(f'{cwd}/access_key/*.json'):
            access_key = glob.glob(f'{cwd}/access_key/*.json')
        else:
            access_key = None
            print('Please enter a valid Google service account access key')

        # Check project name entered; checks len for presence of a project string and checks invalid characters
        if len(project) > 0:
            if bool(re.match("^[A-Za-z0-9-]*$", project)) == True:
                project = project
            else:
                project = None
                print('Invalid project name. Project names may contain letters, numbers, dashes and underscores.')
        else:
            project = None
            print('No project in config.')

        # Check dataset name entered; checks len for presence of a dataset string and checks invalid characters
        if len(dataset) > 0:
            if bool(re.match("^[A-Za-z0-9_]*$", dataset)) == True:
                dataset = dataset
            else:
                print('Invalid dataset name. Dataset names may contain letters, numbers and underscores.')
                dataset = None
        else:
            dataset = None
            print('No dataset in config.')

        return query, start_date, end_date, dataset, access_key