import datetime as dt
# Enter your Twitter Academic API, Mailgun and Google BiqQuery access tokens and rename this file to 'config.py'

class Query():
    query = ''
    start_date = dt.datetime(2022, 1, 1, 0, 0, 0, 0, tzinfo=dt.timezone.utc)
    end_date = dt.datetime(2022, 2, 1, 0, 0, 0, 0, tzinfo=dt.timezone.utc)
    interval_days = 1
    dataset = ''

class Tokens():
    bearer_token = ''

class GBQ():
    gbq_creds = ''
    project_id = ''

class Emails():
    user_email = ''

class Schematype():
    DATA = True
    TCAT = False
    TweetQuery = False
