'''
Contains functions for validating user input
'''

import pandas as pd
import re
import pytz
from google.cloud.exceptions import NotFound
from google.cloud.bigquery.client import Client
from google.auth.exceptions import DefaultCredentialsError

from .set_up_directories import *
from .bq_schema import DATA_schema, TCAT_schema, TweetQuery_schema

pd.options.mode.chained_assignment = None
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)


class ValidateParams:
    def validate_google_access_key(self, cwd, project):
        '''
        Checks that the Google access key supplied is valid for the project specified in config.yml
        '''

        # Check for Google access key; looks for .json service account key in the 'access_key' dir
        for potential_key in glob.glob(f'{cwd}/access_key/*.json'):
            with open(potential_key, 'r') as f:
                contents = f.read()
                if f'"project_id": "{project}"' in contents:
                    return potential_key
                else:
                    # A key for a different project may still work. Try it out.
                    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = potential_key
                    try:
                        bq = Client(project=project)
                        
                        # see if the project we are looking for is in the list of available projects
                        available_projects = bq.list_projects()
                        for auth_project in available_projects:
                            if str.lower(project) == str.lower(auth_project.friendly_name):
                                return potential_key
                    except (TypeError, DefaultCredentialsError):
                        pass
                    
                    print(
                        f'The Google service key provided in {potential_key} is not valid for the project specified in config.yml.'
                        f'\nPlease ensure it is the correct one for the {project} project.')
                    continue  # move on to next key in the folder
        
        print(f'Please enter a valid Google service account access key.')
        exit()

    def validate_search_parameters(self, query, bearer_token, start_date, end_date, project, dataset, bq, schematype, local_json_only=False):
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

        # Check project name entered; checks len for presence of a project string and checks invalid character.
        if len(project) > 0:
            if bool(re.match("^[A-Za-z0-9-]*$", project)) == True:
                project = project
            else:
                project = None
                print('Invalid project name. Project names may contain letters, numbers, dashes and underscores.')
        else:
            project = None
            print('No project in config.')

        # Check dataset name entered; checks len for presence of a dataset string and checks invalid characters. If
        # specified dataset already exists, ask user to confirm if it is ok to append to that dataset. Otherwise, exit.
        if len(dataset) > 0:
            if bool(re.match("^[A-Za-z0-9_]*$", dataset)) == True:
                dataset = dataset
                # Create dataset if one does not exist
                try:
                    ds = bq.get_dataset(dataset)
                    print(f"""The {project}.{dataset} dataset already exists. If you choose to proceed, your data will be appended to this dataset.
                          \n\tOk? y/n""")

                    user_input = input('>>>').lower()

                    if user_input == 'y':
                        # Check that the specified schematype matches the schema of the destination dataset
                        if schematype == 'TweetQuery':
                            table = bq.get_table(f'{project}.{dataset}.tweets_flat')  # Make an API request.
                            chosen_schema = TweetQuery_schema.tweets_flat_schema
                        elif schematype == 'TCAT':
                            table = bq.get_table(f'{project}.{dataset}.tweets')  # Make an API request.
                            chosen_schema = TCAT_schema.tweet_schema
                        else:
                            table = bq.get_table(f'{project}.{dataset}.tweets')  # Make an API request.
                            chosen_schema = DATA_schema.tweet_schema

                        dest_schema = table.schema

                        if len(chosen_schema) == len(dest_schema):
                            dataset = dataset
                        else:
                            print(f'\nTo append to this dataset, please ensure the schema specified in config.yml matches that of {project}.{dataset}.')
                            dataset = None
                    else:
                        exit()

                except NotFound:
                    pass
            else:
                print('Invalid dataset name. Dataset names may contain letters, numbers and underscores.')
                dataset = None
        else:
            dataset = None
            print('No dataset in config.')



        # If any of the above parameters are None, exit program; else, proceed.
        if None in [query, bearer_token, start_date, end_date, project, dataset]:
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

            return query, bearer_token, start_date, end_date, project, dataset, local_json_only

    def validate_project_parameters(self, project, dataset, bq, schematype):
        '''
        Validates project parameters when Option 2 (Process from .json) is selected.
        If parameter is invalid, then parameter=None and program will notify user and exit.
        '''

        query = 'JSON upload'
        start_date = ''
        end_date = ''

        # Check project name entered; checks len for presence of a project string and checks invalid character.
        if len(project) > 0:
            if bool(re.match("^[A-Za-z0-9-]*$", project)) == True:
                project = project
            else:
                project = None
                print('Invalid project name. Project names may contain letters, numbers, dashes and underscores.')
        else:
            project = None
            print('No project in config.')

        # Check dataset name entered; checks len for presence of a dataset string and checks invalid characters. If
        # specified dataset already exists, ask user to confirm if it is ok to append to that dataset. Otherwise, exit.
        if len(dataset) > 0:
            if bool(re.match("^[A-Za-z0-9_]*$", dataset)) == True:
                dataset = dataset
                # Create dataset if one does not exist
                try:
                    ds = bq.get_dataset(dataset)
                    print(f"""The {project}.{dataset} dataset already exists. Your data will be appended to this dataset.
                              \nOk? y/n""")

                    user_input = input('>>>').lower()

                    if user_input == 'y':
                        # Check that the specified schematype matches the schema of the destination dataset
                        if schematype == 'TweetQuery':
                            table = bq.get_table(f'{project}.{dataset}.tweets_flat')  # Make an API request.
                            chosen_schema = TweetQuery_schema.tweets_flat_schema
                        elif schematype == 'TCAT':
                            table = bq.get_table(f'{project}.{dataset}.tweets')  # Make an API request.
                            chosen_schema = TCAT_schema.tweet_schema
                        else:
                            table = bq.get_table(f'{project}.{dataset}.tweets')  # Make an API request.
                            chosen_schema = DATA_schema.tweet_schema

                        dest_schema = table.schema

                        if len(chosen_schema) == len(dest_schema):
                            dataset = dataset
                        else:
                            print(
                                f'\nTo append to this dataset, please ensure the schema specified in config.yml matches that of {project}.{dataset}.')
                            dataset = None
                    else:
                        exit()

                except NotFound:
                    pass
            else:
                print('Invalid dataset name. Dataset names may contain letters, numbers and underscores.')
                dataset = None
        else:
            dataset = None
            print('No dataset in config.')

        return query, start_date, end_date, dataset

