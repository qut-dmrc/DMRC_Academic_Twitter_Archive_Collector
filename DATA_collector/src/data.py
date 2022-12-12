import json
import requests

import pandas as pd
import time

import humanfriendly
from humanfriendly import format_timespan
import traceback
import re

from twarc import Twarc2, expansions
from google.cloud import bigquery
from google.cloud.bigquery.client import Client
from google.cloud.exceptions import NotFound
from google.api_core import exceptions

from .bq_schema import SchemaFuncs
from .notifications import *
from .fields import DATA_fields, TCAT_fields, TweetQuery_fields, TWEET_fields
from .set_up_directories import *
from .validate_params import ValidateParams
from .process_tables import ProcessTweets, ProcessTables

pd.options.mode.chained_assignment = None
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)



def get_pre_search_counts(*args):
    '''
    Runs a Twarc counts search on the query in config.yml when Option 1 (Search Archive) is selected.
    '''

    client = args[0]
    # Run counts_all search
    try:
        count_tweets = client.counts_all(query=args[1], start_time=args[2], end_time=args[3])

        # Append each page of data to list
        tweet_counts_data = []
        for page in count_tweets:
            tweet_counts_data.append(page)

        # Combine all ["meta"]["total_tweet_count"] and sum for total tweets
        tweet_counts = []
        for i in range(len(tweet_counts_data)):
            total = (tweet_counts_data[i]["meta"]["total_tweet_count"])
            tweet_counts.append(total)

        # Get total tweet counts
        archive_search_counts = sum(tweet_counts)

        # Give an estimate of search duration
        ''' Currently saving search duration data and counts to a txt file from which I will work out a good calc'''
        time_estimate = (archive_search_counts*0.4784919736026976)/2
        readable_time_estimate = format_timespan(time_estimate)
    except requests.exceptions.HTTPError:
        print(f"\nThere seems to be an issue with your query that wasn't caught at the validation stage. \nIt might be an invalid bearer token.\nIf you are searching for tweets in a specific language, make sure you are using the correct lang code!")
        exit()


    return archive_search_counts, readable_time_estimate

def calculate_interval(start_date, end_date, archive_search_counts):
    '''
    Calculates an appropriate interval to serve as number of days per json file collected. Helps to keep file sizes and
    memory usage low.
    interval = search_duration * ave_tweets_per_file / archive_search_counts
    '''
    search_duration = (end_date - start_date).days
    if search_duration == 0:
        search_duration = 1
    ave_tweets_per_file = 100000
    archive_search_counts = archive_search_counts
    if archive_search_counts > 0:
        interval = search_duration * ave_tweets_per_file / archive_search_counts
        num_intervals = round(archive_search_counts/ave_tweets_per_file)
        if num_intervals <= 0:
            num_intervals = 1
    else:
        interval = None
        num_intervals = None

    return interval, num_intervals

def collect_archive_data(bq, project, dataset, to_collect, not_to_collect, expected_files, client, subquery, start_date, end_date, csv_filepath, archive_search_counts, tweet_count, query, query_count, schematype):
    '''
    Uses a dictionary containing expected filename, start_date and end-date, generated in set_up_directories.py.
    For each file in the dictionary, a separate query is run, resulting in e.g. 1 file per day if interval = 1.
    This function loops through the expected files if they do not already exist in the 'collected_json' dir.
    Leads to the process_json_data() function.
    '''

    logging.info('Commencing data collection...')
    # Collect archive data one interval at a time using the Twarc search_all endpoint
    if len(to_collect) > 0:
        for a_file in to_collect:
            # logging.info('-----------------------------------------------------------------------------------------')
            logging.info(f'Collecting file {a_file}')
            start, end = expected_files[a_file]
            if type(query) == list:
                logging.info(f'Query {query_count} of {len(list(query))}')
            logging.info(f'Query: {subquery} from {start} to {end}')

            # Twarc search_all
            search_results = client.search_all(query=subquery, start_time=start, end_time=end, max_results=100)

            # Flatten tweet objects and dump to json
            for page in search_results:
                result = expansions.flatten(page)
                for tweet in result:
                    json_object = (json.dumps(tweet))
                    with open(a_file, "a") as f:
                        f.write(json_object + "\n")

            # Start processing collected file
            if os.path.isfile(a_file):
                logging.info(f'Processing tweet data...')
                # Process json data
                tweet_count, list_of_dataframes = process_json_data(a_file, csv_filepath, bq, project, dataset, subquery, start_date, end_date, archive_search_counts, tweet_count, schematype, test=False)

    else:
        print_already_collected(dataset, not_to_collect)
        exit()

def process_json_data(a_file, csv_filepath, bq, project, dataset, subquery, start_date, end_date, archive_search_counts, tweet_count, schematype, test):
    '''
    For each file collected, process 50,000 lines at a time. This keeps memory usage low while processing at a reasonable rate.
    Un-nests each tweet object, flattens main Tweet table, then sorts nested columns into separate, flattened tables.
    All tables are connected to the main Tweet table by either 'tweet_id', 'author_id' or 'poll_id'.
    '''

    # Process json 50,000 lines at a time
    for chunk in pd.read_json(a_file, lines=True, dtype=False, chunksize=50000):
        tweets = chunk
        # Rename 'id' field for clarity.
        try:
            tweets = tweets.rename(columns={'id': 'tweet_id', 'id_str': 'tweet_id'}, errors='ignore')
            tweets['tweet_id'] = tweets['tweet_id'].astype(object)
        except KeyError:
            print("No 'id' or 'id_str' fields found in dataframe. Exiting...")

        # Init ProcessTweets class
        process_tweets = ProcessTweets()

        # Call function to flatten top level tweets and merge with one-to-one nested columns
        tweets_flat = process_tweets.flatten_top_tweet_level(tweets)
        # Start reference_levels_list with tweets_flat only; append lower reference levels to this list
        reference_levels_list = [tweets_flat]
        reference_levels_list = process_tweets.unpack_referenced_tweets(reference_levels_list)
        # Get pre-defined fields from fields.py (DATA class)
        up_a_level_column_list = TWEET_fields.up_a_level_column_list
        # Copy data from reference levels to previous level, as 'referenced_tweet' columns. This creates 'TWEET' table
        TWEETS = process_tweets.move_referenced_tweet_data_up(reference_levels_list, up_a_level_column_list)

        # Address retweet truncation issue
        TWEETS = process_tweets.fix_retweet_truncation(TWEETS)

        logging.info('TWEETS table built')

        logging.info('Unpacking one-to-many nested columns...')

        # Init ProcessTables class
        data_processor = ProcessTables()

        # Pull entities_mentions from TWEETS for building MENTIONS, AUTHOR DESCRIPTION, AUTHOR_URLS
        if 'entities_mentions' in TWEETS.columns:
            entities_mentions = data_processor.extract_entities_data(TWEETS)
        else:
            entities_mentions = pd.DataFrame()

        # Depending on schema chosen, proceed with table building
        if Schematype.DATA == True:
            AUTHOR_DESCRIPTION = data_processor.build_author_description_table(TWEETS, entities_mentions)
            AUTHOR_URLS = data_processor.build_author_urls_table(TWEETS, entities_mentions)
            MEDIA = data_processor.build_media_table(TWEETS)
            POLL_OPTIONS = data_processor.build_poll_options_table(TWEETS)
            CONTEXT_ANNOTATIONS = data_processor.build_context_annotations_table(TWEETS)
            ANNOTATIONS = data_processor.build_annotations_table(TWEETS)
            HASHTAGS = data_processor.build_hashtags_table(TWEETS)
            CASHTAGS = data_processor.build_cashtags_table(TWEETS)
            URLS = data_processor.build_urls_table(TWEETS)
            TWEETS = process_tweets.extract_quote_reply_users(TWEETS, URLS)
            MENTIONS = data_processor.build_mentions_table(entities_mentions)
            INTERACTIONS = data_processor.build_interactions_table(TWEETS, MENTIONS)
            EDIT_HISTORY = data_processor.build_edit_history_table(TWEETS)
        else:
            TWEETS = TWEETS.loc[TWEETS['reference_level'] == '0']
            MENTIONS = data_processor.build_mentions_table(entities_mentions)
            INTERACTIONS = data_processor.build_interactions_table(TWEETS, MENTIONS)
            HASHTAGS = data_processor.build_hashtags_table(TWEETS)
            URLS = data_processor.build_urls_table(TWEETS)
            AUTHOR_DESCRIPTION = AUTHOR_URLS = MEDIA = POLL_OPTIONS = CONTEXT_ANNOTATIONS = ANNOTATIONS = EDIT_HISTORY = None

        # Special case of geo_geo_bbox: convert from column of lists to strings
        if 'geo_geo_bbox' in TWEETS.columns:
            TWEETS['geo_geo_bbox'] = TWEETS['geo_geo_bbox'].fillna('')
            TWEETS['geo_geo_bbox'] = [','.join(map(str, l)) for l in TWEETS['geo_geo_bbox']]

        TWEETS = process_tweets.process_boolean_cols(TWEETS)
        TWEETS = TWEETS.rename(columns={
            'text': 'tweet_text'})\
            .reset_index(drop=True)\
            .reindex(columns=DATA_fields.tweet_column_order) \
            .drop_duplicates()

        TWEETS = process_tweets.fill_blanks_and_nas(TWEETS)

        TWEETS = TWEETS.astype(TWEET_fields.tweet_table_dtype_dict)

        list_of_dataframes = [TWEETS,
                              MEDIA,
                              ANNOTATIONS,
                              CONTEXT_ANNOTATIONS,
                              HASHTAGS,
                              CASHTAGS,
                              URLS,
                              MENTIONS,
                              AUTHOR_DESCRIPTION,
                              AUTHOR_URLS,
                              POLL_OPTIONS,
                              INTERACTIONS,
                              EDIT_HISTORY]

        if test == False:
            # Init SchemaFuncs class
            schema_funcs = SchemaFuncs()

            # Proceed according to schema chosen
            list_of_dataframes, list_of_csv, list_of_tablenames, list_of_schema, tweet_count = schema_funcs.get_schema_type(list_of_dataframes, tweet_count)

            # For each processed json, write to temp csv file
            for tweetframe, csv_file in zip(list_of_dataframes, list_of_csv):
                write_processed_data_to_csv(tweetframe, csv_file, csv_filepath)
            if archive_search_counts > 0:
                percent_collected = tweet_count / archive_search_counts * 100
                logging.info(f'{tweet_count} of {archive_search_counts} tweets collected ({round(percent_collected, 1)}%)')

            # Write temp csv files to BigQuery tables
            push_processed_tables_to_bq(bq, project, dataset, list_of_tablenames, csv_filepath, list_of_csv, subquery, start_date, end_date, list_of_schema, list_of_dataframes, schematype)

    return tweet_count, list_of_dataframes

def write_processed_data_to_csv(tweetframe, csv_file, csv_filepath):
    '''
    Write each generated table to a temporary csv file, to be pushed to Google BigQuery.
    '''
    try:
        if tweetframe is not None:
            if os.path.isfile(csv_filepath + csv_file) == True:
                    logging.info(f'Appending data to existing {csv_file} file...')
                    mode = 'a'
                    header = False
            else:
                logging.info(f'Writing data to new {csv_file} file...')
                mode = 'w'
                header = True

            tweetframe.to_csv(csv_filepath + csv_file,
                              mode=mode,
                              index=False,
                              escapechar='|',
                              header=header)
    except TypeError:
        pass
        print('Test does not produce CSV tables')

def push_processed_tables_to_bq(bq, project, dataset, list_of_tablenames, csv_filepath, list_of_csv, subquery, start_date, end_date, list_of_schema, list_of_dataframes, schematype):
    '''
    Pushes records from temp csv files to Google BigQuery, using dataset specified in config.yml. If dataset does not
    exist, it will be created. Description is query, start_date and end_date. A new description is appended if dataset
    already exists. Description char limit ~ 16000; if limit reached, will not append.
    '''

    logging.info('Pushing tables to Google BigQuery database.')

    tweet_dataset = f"{project}.{dataset}"
    num_tables = len(list_of_tablenames)

    # Create dataset if one does not exist
    try:
        ds = bq.get_dataset(tweet_dataset)
    except NotFound:
        # Dataset does not yet exist
        logging.info(f"Dataset {tweet_dataset} is not found.")
        bq.create_dataset(tweet_dataset)
        ds = bq.get_dataset(tweet_dataset)
        logging.info(f"Created new dataset: {tweet_dataset}.")

    # Update or add dataset description
    try:
        if ds.description:
            ds.description = f"{ds.description}\n[{datetime.now()}] Query: {subquery}, {start_date}, {end_date}"
            bq.update_dataset(ds, ["description"])
            logging.info('Dataset description updated.')
        else:
            ds.description = f"[{datetime.now()}] Query: {subquery}, {start_date}, {end_date}"
            bq.update_dataset(ds, ["description"])
            logging.info('Dataset description added.')
    except exceptions.BadRequest:
        logging.info('Dataset description too long. Description not appended.')
        pass

    # Create table in dataset if one does not exist
    for i in range(num_tables):
        if os.path.isfile(csv_filepath + list_of_csv[i]) == True:
            if schematype != 'TweetQuery':
                tweets_table = list_of_tablenames[i]
            else:
                tweets_table = 'tweets_flat'

            table_id = bigquery.Table(f'{tweet_dataset}.{tweets_table}')

            try:
                bq.get_table(table_id)
            except:
                table = bq.create_table(table_id)
                logging.info(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")

            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,
                schema=list_of_schema[i],
                max_bad_records=10)

            job_config.allow_quoted_newlines = True

            with open(csv_filepath + list_of_csv[i], 'rb') as tweet_fh:
                for attempt in range(1, 11):
                    try:
                        job = bq.load_table_from_file(tweet_fh, tweet_dataset + '.' + tweets_table,
                                                      job_config=job_config, rewind=True)
                        job.result()
                    except exceptions.InternalServerError as e:
                        print(e)
                        time.sleep(3*attempt)
                        logging.info(f"Internal server error - attempt {attempt} of 10...")
                        logging.info(f"Waiting {3*attempt} seconds...")
                        continue
                    else:
                        break

            table = bq.get_table(table_id)
            try:
                logging.info(f"Loaded {len(list_of_dataframes[i])} rows and {len(table.schema)} columns "
                             f"to {table.project}.{table.dataset_id}.{table.table_id}")
            except TypeError:
                continue

    file_list = glob.glob(csv_filepath + '*csv')
    # Iterate over the list of filepaths & remove each file.
    for file_path in file_list:
        os.remove(file_path)

    return table

    logging.info('-----------------------------------------------------------------------------------------')

def query_total_record_count(table, bq):
    '''
    Gets total count of records in Google BigQuery table. If shematype = DATA, query only the level 0 tweets. Else,
    query entire table (since TCAT and TQ do not have referenced tweets).
    '''

    # Query total rows in dataset
    try:
        if Schematype.DATA == True:
            # Query number of level 0 tweets in dataset
            counts_query_string = f"""
                SELECT
                *
                FROM `{table.project}.{table.dataset_id}.tweets`
                WHERE reference_level = '0'
                """
            lv0_tweet_count = (bq.query(counts_query_string).result()).total_rows
        elif Schematype.TCAT == True:
            counts_query_string = f"""
                SELECT
                *
                FROM `{table.project}.{table.dataset_id}.tweets`
                """
            lv0_tweet_count = (bq.query(counts_query_string).result()).total_rows
        else:
            counts_query_string = f"""
                 SELECT
                 *
                 FROM `{table.project}.{table.dataset_id}.tweets_flat`
                 """
            lv0_tweet_count = (bq.query(counts_query_string).result()).total_rows
    except:
        logging.info("Tweets table too large to retrieve row counts from BigQuery database.")
        lv0_tweet_count = 'Undeterminable'

    return lv0_tweet_count

def capture_error_string(error, error_filepath):
    '''
    Gets traceback info for any exception, to be saved to error log and emailed to admin for investigation.
    '''

    error_string = repr(error)
    logging.info(error_string)
    traceback_info = traceback.format_exc()
    logging.info(traceback_info)
    error_time = re.sub("[^0-9]", "", str(datetime.now().replace(microsecond=0)))
    text_file = open(f'{error_filepath}error_log_{error_time}.txt', "w")
    text_file.write(traceback_info)
    text_file.close()
    logging.info('Error log written to file.')

    return traceback_info

def notify_completion(bq, search_start_time, project, dataset, start_date, end_date, option_selection, archive_search_counts, subquery, interval):
    '''
    Calculates duration of search/processing and sends emails to user notifying them of completion.
    '''

    search_end_time = datetime.now()
    search_duration = (search_end_time - search_start_time)
    readable_duration = humanfriendly.format_timespan(search_duration)

    table = 1
    if table > 0:
        table_id = bigquery.Table(f'{project}.{dataset}.tweets')
        try:
            table = bq.get_table(table_id)
        except:
            table_id = bigquery.Table(f'{project}.{dataset}.tweets_flat')
            table = bq.get_table(table_id)
        lv0_tweet_count = query_total_record_count(table, bq)
        time.sleep(3)
        print_completion(start_date, end_date,
                              lv0_tweet_count, search_start_time, search_end_time, readable_duration,
                              number_rows=table.num_rows, project_name=table.project, dataset_name=table.dataset_id,
                              query=subquery)
        # logging.info('Completion email sent to user.')
    else:
        time.sleep(10)
        print_no_results(start_date, end_date, query=subquery)
        # logging.info('No results! Email sent to user.')

    if option_selection == '2':
        logging.info('json processing complete!')
    else:
        logging.info('Archive search complete!')

        # Store search data for working out time estimates
        with open('duration_log.txt', 'a') as f:
            f.write(
                f'{search_start_time}, {search_end_time}, {search_duration}, {archive_search_counts}, {interval}\n')



def run_DATA():
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
    Vodden, Laura. (2022). DMRC Academic Twitter Archive (DATA) Collector. Version 1 
    (Beta). Brisbane: Digital Media Research Centre, Queensland University of Technology. 
    https://github.com/qut-dmrc/DMRC_Academic_Twitter_Archive_Collector.git

    --------------------------------------------------------------------------
    \n""")

    time.sleep(2)

    print("""
    Please make a selection from the following options:
    \n
        1 - Search Archive
        2 - Process JSON File(s)
        
    """)

    option_selection = input('>>>')

    try:
        if option_selection == "1":
            # Set search parameters
            query = Query.query
            bearer_token = Tokens.bearer_token
            start_date = Query.start_date
            end_date = Query.end_date
            project = GBQ.project_id
            dataset = GBQ.dataset


            # Init ValidateParams class
            valdate_params = ValidateParams()

            # Validate search parameters
            query, bearer_token, access_key, start_date, end_date, project, dataset = valdate_params.validate_search_parameters(query, bearer_token, start_date, end_date, project, dataset)

            # Set schema type
            schema_funcs = SchemaFuncs()
            schematype = schema_funcs.set_schema_type(Schematype)

            # Initiate a Twarc client instance
            client = Twarc2(bearer_token=bearer_token)

            # Pre-search archive counts
            # subquery = query
            print("Getting Tweet count estimate for your query. Please wait...")
            archive_search_counts, readable_time_estimate = get_pre_search_counts(client, query, start_date, end_date)

            if archive_search_counts > 0:

                interval, num_intervals = calculate_interval(start_date, end_date, archive_search_counts)
                # Print search results for user and ask to proceed
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
        Proceed? y/n""")


                user_proceed = input('>>>').lower()

                if user_proceed == 'y':
                    sleep(1)
                    set_up_directories(logfile_filepath, dir_name, folder, json_filepath, csv_filepath, error_filepath)

                    # Access BigQuery
                    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = access_key[0]
                    bq = Client(project=project)

                    # Set table variable to none, if it gets a value it can be queried
                    table = 0
                    tweet_count = 0
                    query_count = 1

                    try:
                        # Get current datetime for calculating duration
                        search_start_time = datetime.now()
                        # to_collect, expected files tell the program what to collect and what has already been collected
                        to_collect, not_to_collect, expected_files = set_up_expected_files(start_date, end_date, json_filepath, option_selection, dataset, interval)
                        # Call function collect_archive_data()
                        collect_archive_data(bq, project, dataset, to_collect, not_to_collect, expected_files, client, query, start_date, end_date, csv_filepath, archive_search_counts, tweet_count, query, query_count, schematype)
                        # Notify user of completion
                        notify_completion(bq, search_start_time, project, dataset, start_date, end_date, option_selection, archive_search_counts, subquery=query, interval=interval)

                    except Exception as error:
                        traceback_info = capture_error_string(error, error_filepath)
                        logging.info(traceback_info)
                        print_error(dataset, traceback_info)
                        # logging.info('Error email sent to admin.')
                else:
                    exit()

            else:
                print_no_results(start_date, end_date, query)

        elif option_selection == "lv":

            # Set search parameters
            query = Query.query_list
            bearer_token = Tokens.bearer_token
            start_date = Query.start_date
            end_date = Query.end_date
            project = GBQ.project_id
            dataset = GBQ.dataset


            # Init ValidateParams class
            valdate_params = ValidateParams()

            # Validate search parameters
            query, bearer_token, access_key, start_date, end_date, project, dataset = valdate_params.validate_search_parameters(query, bearer_token, start_date, end_date, project, dataset)

            # Init SchemaFuncs class
            schema_funcs = SchemaFuncs()

            # Set schema type
            schematype = schema_funcs.set_schema_type(Schematype)

            # Initiate a Twarc client instance
            client = Twarc2(bearer_token=bearer_token)

            # Print queries for user to accept
            print('''
            You are about to search the following queries:\n''')
            for item in query:
                print(item)
            print(f'''\n
            Total queries: {len(query)}\n\n\nProceed? y/n''')

            user_proceed = input('>>>').lower()

            if user_proceed == 'y':
                sleep(1)
                set_up_directories(logfile_filepath, dir_name, folder, json_filepath, csv_filepath, error_filepath)

                # Access BigQuery
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = access_key[0]
                bq = Client(project=project)

                # Set table variable to none, if it gets a value it can be queried
                table = 0
                tweet_count = 0
                query_count = 0

                try:
                    for subquery in query:
                        query_count = query_count + 1

                        # Get current datetime for calculating duration
                        search_start_time = datetime.now()
                        if 'url' in subquery:
                            subquery_formatted = subquery
                        else:
                            subquery_formatted = f'@{subquery} OR from:{subquery}'

                        # Pre-search archive counts
                        logging.info('-----------------------------------------------------------------------------------------')
                        logging.info(f'Getting tweet counts for query: {subquery_formatted}')
                        archive_search_counts, readable_time_estimate = get_pre_search_counts(client, subquery_formatted, start_date, end_date)

                        logging.info(f'Archive counts: {archive_search_counts}')

                        if archive_search_counts > 0:
                            # to_collect, expected files tell the program what to collect and what has already been collected
                            interval, num_intervals = calculate_interval(start_date, end_date, archive_search_counts)
                            to_collect, not_to_collect, expected_files = set_up_expected_files(start_date, end_date, json_filepath, option_selection, subquery, interval)

                            # Call function collect_archive_data()
                            collect_archive_data(bq, project, dataset, to_collect, not_to_collect, expected_files, client, subquery_formatted, start_date, end_date, csv_filepath, archive_search_counts, tweet_count, query, query_count, schematype)
                            # Notify user of completion
                            notify_completion(bq, search_start_time, project, dataset, start_date, end_date, option_selection, archive_search_counts, subquery=subquery_formatted, interval=interval)
                        else:
                            logging.info(f'Archive_counts: {archive_search_counts}')

                except Exception as error:
                    traceback_info = capture_error_string(error, error_filepath)
                    logging.info(traceback_info)
                    print_error(dataset, traceback_info)
                    # logging.info('Error email sent to admin.')
            else:
                exit()

        elif option_selection == '2':

            # Set parameters
            query = 'Process from json file'
            project = GBQ.project_id
            dataset = GBQ.dataset

            archive_search_counts = 0
            tweet_count = 0

            # Init ValidateParams class
            valdate_params = ValidateParams()

            # Validate parameters
            query, start_date, end_date, dataset, access_key = valdate_params.validate_project_parameters(project, dataset)

            # Init SchemaFuncs class
            schema_funcs = SchemaFuncs()

            # Set schema type
            schematype = schema_funcs.set_schema_type(Schematype)

            # subquery = query

            # Get json files from 'json_input_files' directory
            json_input_files = get_json_input_files()

            # Print files to be processed for user to accept
            print(f'''
            The following files will be processed and uploaded to {project}.{dataset}:''')
            for item in json_input_files:
                print(f'''
                {item}''')
            print(f'''\n
            Total files to process: {len(json_input_files)}\n\n\n\nProceed? y/n''')

            user_proceed = input('>>>').lower()

            if user_proceed == 'y':
                test = False
                # Set up directories
                set_up_directories(logfile_filepath, dir_name, folder, json_filepath, csv_filepath, error_filepath)

                logging.info('-----------------------------------------------------------------------------------------')
                logging.info('Processing from existing json file(s)...')

                # Access BigQuery
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = access_key[0]
                bq = Client(project=project)

                # Get current datetime for calculating duration
                search_start_time = datetime.now()
                filecount = 1
                for a_file in json_input_files:
                    logging.info('-----------------------------------------------------------------------------------------')
                    logging.info(f'Processing file {a_file}')
                    logging.info(f'File {filecount} of {len(json_input_files)}')
                    filecount = filecount + 1

                    # For each interval (file), process json
                    tweet_count = process_json_data(a_file, csv_filepath, bq, project, dataset, query, start_date, end_date, archive_search_counts, tweet_count, schematype, test)
                    # Notify user of completion
                    notify_completion(bq, search_start_time, project, dataset, start_date, end_date, option_selection, archive_search_counts, subquery=query, interval=0)
            else:
                exit()
        else:
            print('Invalid selection')

    except Exception as error:
        traceback_info = capture_error_string(error, error_filepath)
        logging.info(traceback_info)
        print_error(dataset, traceback_info)
