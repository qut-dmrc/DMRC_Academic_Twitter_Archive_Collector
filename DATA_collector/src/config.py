'''
This file gets data from config.yml and assigns them to variables.
'''

import datetime as dt
import os
import yaml

wd = os.getcwd()

with open(f'{wd}/config/config.yml') as f:
    config = yaml.load(f, Loader=yaml.FullLoader)


class Query():
    query = config['query']
    # start_date = config['start_date']
    start_date = dt.datetime.fromisoformat(config['start_date'])
    # end_date = config['end_date']
    end_date = dt.datetime.fromisoformat(config['end_date'])
    interval_days = config['interval_days']
    query_list = config['query_list']

class Tokens():
    bearer_token = config['bearer_token']

class GBQ():
    project_id = config['project_id']
    dataset = config['dataset']

class Emails():
    user_email = config['user_email']

class Schematype():
    DATA = config['DATA']
    TCAT = config['TCAT']
    TweetQuery = config['TweetQuery']

