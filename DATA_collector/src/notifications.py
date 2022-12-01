
from .config import *



def print_completion(start_date, end_date, lv0_tweet_count, search_start_time, search_end_time, readable_duration, number_rows, project_name, dataset_name, query):
    print(f"""
Your DATA search is complete! Please find details relating to your search below:
        \n
        ----------------
        Query: {query}
        Query start: {start_date}
        Query end: {end_date}
        \n
        Level 0 tweets in dataset: {lv0_tweet_count}
        Total tweets in dataset (including referenced tweets): {number_rows}
        \n
        Dataset: {dataset_name}
        Data location: {project_name}.{dataset_name}
        ----------------
        \n
This process commenced at {search_start_time.strftime("%Y-%m-%d %H:%M:%S")}, and completed at {search_end_time.strftime("%Y-%m-%d %H:%M:%S")}. The search took {readable_duration}
    """)


def print_no_results(start_date, end_date, query):
    print(f"""
\n
Your DATA search produced 0 results. 
This could be because there are no Tweets to collect that match your search parameters:

        Query: {query}
        Query start: {start_date}
        Query end: {end_date}

Exiting...
\n
    """)


def print_error(dataset, traceback_info):
    print(f"""
\n
Your {dataset} DATA search has encountered the following error: 
\n
    {traceback_info}
    """)

def print_already_collected(dataset, not_to_collect):
    if len(not_to_collect) == 1:
        th = 'this file'
    else:
        th = 'these files'

    print(f"""
\n
It looks like you have already collected, or partially collected, these data. 
Please check your DATA_collector/{dataset}/collected_json directory for the following files:
""")

    for i in not_to_collect:
        i = i.split('/')[-1]
        print(f"""
        {i}
        """)

    print(f"""
Either modify your search or remove {th}.

Exiting...
    """)