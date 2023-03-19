import time

from .config import *


def print_welcome_screen():
    print("""
        \n
                                    ___________
                                      |_____|  \  
                                      | o o |   *
                                       \ V / 
                                       /   \ 
                                      |     |
                                      |     |
                                  -----m---m-----
                                  |  Thank You! |
                                  ---------------
                                        \/   

        \n
        Thank you for using the DMRC Academic Twitter Archive (DATA) Collector!
        \n
        If you use this tool to collect data for a publication, please cite me as: 
        \n
        Vodden, Laura. (2022). DMRC Academic Twitter Archive (DATA) Collector. Version 1. 
        Brisbane: Digital Media Research Centre, Queensland University of Technology. 
        https://github.com/qut-dmrc/DMRC_Academic_Twitter_Archive_Collector.git

        --------------------------------------------------------------------------
        \n""")

    time.sleep(1)

    print("""
        Please make a selection from the following options:
        \n
            1 - Search Archive
            2 - Process JSON File(s)

        """)

    option_selection = input('>>>')

    return option_selection

def get_user_confirmation_no_gbq():
    
    print("Your config has the 'local_json_only' flag set. Tweets will NOT be uploaded to BigQuery.\n\nDo you wish to proceed? y/n\n")

    user_proceed = input('>>>').lower()

    return user_proceed

def get_user_confirmation_string_search(query, start_date, end_date, project, dataset, schematype, archive_search_counts, num_intervals):
    print(f"""
    \n
    Please check the below details carefully, and ensure you have enough room in your academic project bearer token quota!
    \n
    Your query: {query}
    Start date: {start_date}
    End date: {end_date}
    Destination database: {project}.{dataset}
    Schema type: {schematype}
    \n
    Your archive search will collect approximately {archive_search_counts} tweets (upper estimate).
    The collection will be distributed across approximately {num_intervals} json files.
    \n
    ** Remember to monitor the space on your hard drive! **
    \n 
    \n
    \tProceed? y/n""")

    user_proceed = input('>>>').lower()

    return user_proceed

def get_user_confirmation_batch_counts(query, start_date, end_date, project, dataset, schematype):
    print(f'''
        -----------------------------------------
        You are about to search the following queries:\n''')
    for item in query:
        print(f'\t\t\t\t{item}\n')

    print(f'''
        Total queries: {len(query)}''')

    print(f'''
        -----------------------------------------
        \n
        Please check the below details carefully!
        \n
        Start date: {start_date}
        End date: {end_date}
        Destination database: {project}.{dataset}
        Schema type: {schematype}
                    \n\n\n\tWould you like to run a batch counts analysis for these queries before your search (this will not affect your API quota)?
                    \n\tThis could take a while if you have many queries.
                    \n\ty/n\n''')

    user_proceed = input('>>>').lower()

    return user_proceed

def get_user_confirmation_batch_search(dataset):
    print(f"""
                \n\t\tWould you like to search these queries?
                \n\t\t  ** Remember to monitor the space on your hard drive! **
                \n\t\ty/n
                """)

    user_proceed = input('>>>').lower()

    return user_proceed

def get_user_confirmation_json_process(project, dataset, json_input_files):
    # Print files to be processed for user to accept
    print(f'''
    The following files will be processed and uploaded to {project}.{dataset}:''')
    for item in json_input_files:
        print(f'''
        {item}''')
    print(f'''\n
    Total files to process: {len(json_input_files)}\n\n\n\n\tProceed? y/n''')

    user_proceed = input('>>>').lower()

    return user_proceed

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