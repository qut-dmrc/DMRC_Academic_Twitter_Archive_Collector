'''
This file gets data from config.yml and assigns them to variables.
'''

import datetime as dt
import os
import yaml

wd = os.getcwd()

with open(f'{wd}/config/config.yml', encoding='utf-8') as f:
    config = yaml.load(f, Loader=yaml.FullLoader)


class Query():
    if type(config['query']) == str:
        query = config['query']
        query_list = None
    else:
        query_list = config['query']
        query = None

    start_date = dt.datetime.fromisoformat(config['start_date'])
    end_date = dt.datetime.fromisoformat(config['end_date'])



class Tokens():
    bearer_token = config['bearer_token']

class GBQ():
    project_id = config['project_id']
    dataset = config['dataset']

# class Emails():
#     user_email = config['user_email']

class Schematype():
    DATA = config['DATA']
    TCAT = config['TCAT']
    TweetQuery = config['TweetQuery']

