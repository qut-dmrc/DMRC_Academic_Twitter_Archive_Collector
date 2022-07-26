import datetime as dt
# Enter your Twitter Academic API, Mailgun and Google BiqQuery access tokens and rename this file to 'config.py'

class Query():
    query = 'masterchefau'
    start_date = dt.datetime(2022, 4, 11, 0, 0, 0, 0, tzinfo=dt.timezone.utc)
    end_date = dt.datetime(2022, 7, 19, 0, 0, 0, 0, tzinfo=dt.timezone.utc)
    interval_days = 1

class Tokens():
    bearer_token = 'AAAAAAAAAAAAAAAAAAAAAIFuUAEAAAAAePHReHjdSnm%2Fn27vtEP8wCZR%2F7o%3DkahBo4Qdqz5ApNJf3tsHdnY4gihoNKZdBxbfamf71rXnLYDTuS'

class GBQ():
    project_id = 'dmrc-data'
    dataset = 'lv_test'

class Emails():
    user_email = 'laura.vodden@outlook.com'

class Schematype():
    DATA = True
    TCAT = False
    TweetQuery = False
