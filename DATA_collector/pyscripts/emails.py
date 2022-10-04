import requests
from .config import *


mailgun_domain = 'sandbox878e4b17425b43b18b208fb54533aafa.mailgun.org'
mailgun_key = 'key-e281f7193b94d71f446b26b0f7875122'


def send_completion_email(mailgun_domain, mailgun_key, query, start_date, end_date, lv0_tweet_count, search_start_time, search_end_time, readable_duration, number_rows, project_name, dataset_name):
    requests.post(
        f"https://api.mailgun.net/v3/{mailgun_domain}/messages",
        auth=("api", mailgun_key),
        data={"from": f"DMRC Academic Twitter App <mailgun@{mailgun_domain}>",
              "to": [f'{Emails.user_email}, laura.vodden@outlook.com'],
              "subject": "Twitter archive search complete",
              "text": f"""Hello!


Your DATA search is complete. Please find details relating to your search below:


    ----------------
    Query: {query}
    Query start: {start_date}
    Query end: {end_date}

    Tweets matching search criteria: {lv0_tweet_count}
    Total tweets in dataset (including referenced tweets): {number_rows}

    Dataset: {dataset_name}
    Data location: {project_name}.{dataset_name}
    ----------------


Your search commenced at {search_start_time.strftime("%Y-%m-%d %H:%M:%S")}, and completed at {search_end_time.strftime("%Y-%m-%d %H:%M:%S")}. The search took {readable_duration}


Best regards,


The DMRC Academic Twitter App (DATA) \U0001F916

    """})


def send_no_results_email(mailgun_domain, mailgun_key, query, start_date, end_date):
    requests.post(
        f"https://api.mailgun.net/v3/{mailgun_domain}/messages",
        auth=("api", mailgun_key),
        data={"from": f"DMRC Academic Twitter App <mailgun@{mailgun_domain}>",
              "to": [Emails.user_email],
              "subject": "No results for your DATA search",
              "text": f"""Hello!


Your DATA search produced 0 results. This could be because there are no Tweets to collect, or there could be something wrong. Please find details relating to your search below:

        ----------------
        Query: {query}
        Query start: {start_date}
        Query end: {end_date}
        ----------------

Please double check your search parameters, or contact Laura Vodden at laura.vodden@qut.edu.au for assistance.



Best regards,


The DMRC Academic Twitter App (DATA) \U0001F916

    """})


def send_error_email(mailgun_domain, mailgun_key, dataset_name, traceback_info):
    requests.post(
        f"https://api.mailgun.net/v3/{mailgun_domain}/messages",
        auth=("api", mailgun_key),
        data={"from": f"DMRC Academic Twitter App <mailgun@{mailgun_domain}>",
              "to": ["laura.vodden@outlook.com"],
              "subject": "Error encountered",
              "text": f"""Your {dataset_name} DATA search has encountered the following error: 

    {traceback_info}"""})
