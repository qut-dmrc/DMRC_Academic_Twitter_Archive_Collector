import datetime as dt
import os
# Enter your Twitter Academic API, Mailgun and Google BiqQuery access tokens and rename this file to 'config.py'
# For Tweet Volume Analyser, only bearer_token is required
# ***Full Archive Search will not work***

class Query():
    query = 'rebelwilson'
    start_date = dt.datetime(2021, 6, 1, 0, 0, 0, 0, tzinfo=dt.timezone.utc)
    end_date = dt.datetime(2021, 6, 5, 0, 0, 0, 0, tzinfo=dt.timezone.utc)
    interval_days = 1
    dataset = 'rebelwilson2'

class Tokens():
    bearer_token = 'AAAAAAAAAAAAAAAAAAAAAIFuUAEAAAAAePHReHjdSnm%2Fn27vtEP8wCZR%2F7o%3DkahBo4Qdqz5ApNJf3tsHdnY4gihoNKZdBxbfamf71rXnLYDTuS'
    mailgun_domain = 'sandbox878e4b17425b43b18b208fb54533aafa.mailgun.org'
    mailgun_key = 'key-e281f7193b94d71f446b26b0f7875122'

class GBQ():
    gbq_creds = os.environ['google_env_creds']
    project_id = 'dmrc-data'
