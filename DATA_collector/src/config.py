'''
This file gets data from config.yml and assigns them to variables.
'''

import datetime as dt
import os
import yaml

wd = os.getcwd()

try:
    with open(f'{wd}/config/config.yml', encoding='utf-8') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
except FileNotFoundError:
    print("\nCannot find config file - please ensure you have renamed config_template.yml to config.yml!")
    exit()
except yaml.YAMLError:
    print("\nDetected an issue with your config.")




class Query():
    try:
        if type(config['query']) == str:
            query = config['query']
        else:
            query = config['query']
    except NameError:
        print(
            "\nThere is a problem with your query."
            "\nEnsure your query is structured correctly and your syntax is correct."
            "\nIf you have a list of queries, ensure there are no extra commas between queries.")
        exit()

    start_date = dt.datetime.fromisoformat(config['start_date'])
    end_date = dt.datetime.fromisoformat(config['end_date'])


class Tokens():
    bearer_token = config['bearer_token']


class GBQ():
    project_id = config['project_id']
    dataset = config['dataset']
    
    local_json_only = config.get('local_json_only', False)


# class Emails():
#     user_email = config['user_email']


class Schematype():
    DATA = config['DATA']
    TCAT = config['TCAT']
    TweetQuery = config['TweetQuery']

