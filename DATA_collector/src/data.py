import json

import pandas as pd
import numpy as np
import time

import humanfriendly
from humanfriendly import format_timespan
import traceback
import re
import pytz

from twarc import Twarc2, expansions
from google.cloud import bigquery
from google.cloud.bigquery.client import Client
from google.cloud.exceptions import NotFound
from google.api_core import exceptions

from .bq_schema import DATA_schema, TCAT_schema, TweetQuery_schema
from .emails import *
from .fields import DATA_fields, TCAT_fields, TweetQuery_fields, TWEET_fields
from .set_up_directories import *

pd.options.mode.chained_assignment = None
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)


# ----------------------------------------------------------------------------------------------------------------------
def get_pre_search_counts(*args):
    '''
    Runs a Twarc counts search on the query in config.yml when Option 1 (Search Archive) is selected.
    '''

    client = args[0]
    # Run counts_all search
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
    ave_tweets_per_file = 150000
    archive_search_counts = archive_search_counts
    interval = search_duration * ave_tweets_per_file / archive_search_counts
    num_intervals = round(archive_search_counts/150000)
    if num_intervals <= 0:
        num_intervals = 1
    return interval, num_intervals

def collect_archive_data(bq, project, dataset, to_collect, expected_files, client, subquery, start_date, end_date, csv_filepath, archive_search_counts, tweet_count, query, query_count, schematype):
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
            logging.info('-----------------------------------------------------------------------------------------')
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
                tweet_count = process_json_data(a_file, csv_filepath, bq, project, dataset, subquery, start_date, end_date, archive_search_counts, tweet_count, schematype)

    else:
        logging.info('Files are already in the collected_json directory!')

def process_json_data(a_file, csv_filepath, bq, project, dataset, subquery, start_date, end_date, archive_search_counts, tweet_count, schematype):
    '''
    For each file collected, process 40,000 lines at a time. This keeps memory usage low while processing at a reasonable rate.
    Un-nests each tweet object, flattens main Tweet table, then sorts nested columns into separate, flattened tables.
    All tables are connected to the main Tweet table by either 'tweet_id', 'author_id' or 'poll_id'.
    '''

    # Process json 40,000 lines at a time
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
                              URLS,
                              MENTIONS,
                              AUTHOR_DESCRIPTION,
                              AUTHOR_URLS,
                              POLL_OPTIONS,
                              INTERACTIONS,
                              EDIT_HISTORY]

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

    return tweet_count

def write_processed_data_to_csv(tweetframe, csv_file, csv_filepath):
    '''
    Write each generated table to a temporary csv file, to be pushed to Google BigQuery.
    '''

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
        send_completion_email(mailgun_domain, mailgun_key, start_date, end_date,
                              lv0_tweet_count, search_start_time, search_end_time, readable_duration,
                              number_rows=table.num_rows, project_name=table.project, dataset_name=table.dataset_id,
                              query=subquery)
        logging.info('Completion email sent to user.')
    else:
        time.sleep(10)
        send_no_results_email(mailgun_domain, mailgun_key, start_date, end_date, query=subquery)
        logging.info('No results! Email sent to user.')


    if option_selection == '2':
        logging.info('json processing complete!')
    else:
        logging.info('Archive search complete!')

        # Store search data for working out time estimates
        with open('duration_log.txt', 'a') as f:
            f.write(
                f'{search_start_time}, {search_end_time}, {search_duration}, {archive_search_counts}, {interval}\n')

class ValidateParams:

    def validate_search_parameters(self, query, bearer_token, start_date, end_date, project, dataset):
        '''
        Validates search parameters when Option 1 (Search Archive) selected.
        Unable to check bearer token validity beyond ensuring it is in config.yml; checks for presence only
        '''

        utc = pytz.UTC

        print("""
        Validating your config...
        """)

        # Search query length; if str query, check min and max len of query. Else, if list query, check min len.
        if type(query) == str:
            if len(query) in range(1, 1024):
                query = query
            elif len(query) < 1:
                print('Please enter a search query.')
                query = None
            else:
                query = None
                print(
                    f'Query is too long ({len(query)} characters). Please shorten it to 1024 characters or fewer and retry.')
        else:
            if len(query) < 1:
                print('Please enter a search query.')
                query = None

        # Check bearer token entered; only checks len for presence of a bearer token
        if len(bearer_token) > 0:
            bearer_token = bearer_token
        else:
            bearer_token = None
            print('Please enter a valid bearer token for Twitter API access.')

        # Check for Google access key; looks for .json service account key in the 'access_key' dir
        if glob.glob(f'{cwd}/access_key/*.json'):
            access_key = glob.glob(f'{cwd}/access_key/*.json')
        else:
            access_key = None
            print('Please enter a valid Google service account access key')

        # Ensure start date is in the past, but must be before end date
        if start_date < end_date:
            start_date = start_date
        elif start_date > utc.localize(dt.datetime.now()):
            start_date = None
            print('Start date cannot be in the future.')
        else:
            start_date = None
            print('Start date cannot be after end date.')

        # Ensure end date is in the past
        if end_date < utc.localize(dt.datetime.now()):
            end_date = end_date
        else:
            end_date = None
            print('End date cannot be in the future.')

        # Check project name entered; checks len for presence of a project string and checks invalid characters
        if len(project) > 0:
            if bool(re.match("^[A-Za-z0-9-]*$", project)) == True:
                project = project
            else:
                project = None
                print('Invalid project name. Project names may contain letters, numbers, dashes and underscores.')
        else:
            project = None
            print('No project in config.')

        # Check dataset name entered; checks len for presence of a dataset string and checks invalid characters
        if len(dataset) > 0:
            if bool(re.match("^[A-Za-z0-9_]*$", dataset)) == True:
                dataset = dataset
            else:
                print('Invalid dataset name. Dataset names may contain letters, numbers and underscores.')
                dataset = None
        else:
            dataset = None
            print('No dataset in config.')


        # If any of the above parameters are None, exit program; else, proceed.
        if None in [query, bearer_token, access_key, start_date, end_date, project, dataset]:
            print("""
            \n 
            Exiting...
            \n""")
            exit()
        else:
            print("""
            \n
            Config input valid!
            \n""")

            return query, bearer_token, access_key, start_date, end_date, project, dataset

    def validate_project_parameters(self, project, dataset):
        '''
        Validates project parameters when Option 2 (Process from .json) is selected.
        '''

        query = 'JSON upload'
        start_date = ''
        end_date = ''

        # Check for Google access key; looks for .json service account key in the 'access_key' dir
        if glob.glob(f'{cwd}/access_key/*.json'):
            access_key = glob.glob(f'{cwd}/access_key/*.json')
        else:
            access_key = None
            print('Please enter a valid Google service account access key')

        # Check project name entered; checks len for presence of a project string and checks invalid characters
        if len(project) > 0:
            if bool(re.match("^[A-Za-z0-9-]*$", project)) == True:
                project = project
            else:
                project = None
                print('Invalid project name. Project names may contain letters, numbers, dashes and underscores.')
        else:
            project = None
            print('No project in config.')

        # Check dataset name entered; checks len for presence of a dataset string and checks invalid characters
        if len(dataset) > 0:
            if bool(re.match("^[A-Za-z0-9_]*$", dataset)) == True:
                dataset = dataset
            else:
                print('Invalid dataset name. Dataset names may contain letters, numbers and underscores.')
                dataset = None
        else:
            dataset = None
            print('No dataset in config.')

        return query, start_date, end_date, dataset, access_key

class SchemaFuncs:

    def set_schema_type(self, Schematype):
        '''
        Sets the schema type as defined in config.yml.
        '''

        if Schematype.DATA == True:
            schematype = 'DATA'
        elif Schematype.TCAT == True:
            schematype = 'TCAT'
        else:
            schematype = 'TweetQuery'

        return schematype

    def get_schema_type(self, list_of_dataframes, tweet_count):
        '''
        Select lists of dataframes, csvs, tablenames and schema depending on the schema indicated in config.yml.
        '''

        # Init SchemaTransform class
        transform_schema = SchemaTransform()

        if Schematype.DATA == True:
            list_of_dataframes = list_of_dataframes
            list_of_csv = DATA_schema.list_of_csv
            list_of_tablenames = DATA_schema.list_of_tablenames
            list_of_schema = DATA_schema.list_of_schema
            tweet_count = len(list_of_dataframes[0].loc[list_of_dataframes[0]['reference_level'] == '0']) + tweet_count
        elif Schematype.TCAT == True:
            list_of_dataframes = transform_schema.transform_DATA_to_TCAT(list_of_dataframes)
            list_of_csv = TCAT_schema.list_of_csv
            list_of_tablenames = TCAT_schema.list_of_tablenames
            list_of_schema = TCAT_schema.list_of_schema
            tweet_count = len(list_of_dataframes[0]) + tweet_count
        else:
            list_of_dataframes = transform_schema.transform_DATA_to_TQ(list_of_dataframes)
            list_of_csv = TweetQuery_schema.list_of_csv
            list_of_tablenames = TweetQuery_schema.list_of_tablenames
            list_of_schema = TweetQuery_schema.list_of_schema
            tweet_count = len(list_of_dataframes[0].drop_duplicates(subset=['id'])) + tweet_count

        return list_of_dataframes, list_of_csv, list_of_tablenames, list_of_schema, tweet_count

class ProcessTweets:

    def flatten_top_tweet_level(self, tweets):
        '''
        Flattens main Tweet table after moving nested columns from dataframe. Gives a reference_level value of '0' to each record.
        One-to-one nested columns are merged with the main table.
        One-to-many nested columns are set aside to be processed separately.
        '''

        logging.info('Flattening one-to-one nested columns...')

        # List of nested columns
        nested_cols_list = ['entities',
                            'public_metrics',
                            'author',
                            'in_reply_to_user',
                            'attachments',
                            'geo',
                            '__twarc']

        # Create empty list to hold flattened one-to-one auxiliary dataframes for merging with main tweet dataframe
        aux_df_list = []
        # Isolate each nested column, flatten, to dataframe and append to aux_df_list
        for nested_col in nested_cols_list:
            if nested_col in tweets.columns:
                temp_df = tweets[['tweet_id', nested_col]]\
                    .dropna()\
                    .reset_index(drop=True)
                temp_df_tweet_id = temp_df['tweet_id']
                temp_df = pd.json_normalize(temp_df[nested_col])\
                    .add_prefix(f'{nested_col}_')
                temp_df = pd.concat([temp_df_tweet_id, temp_df], axis=1)
                nested_col_df = temp_df

                aux_df_list.append(nested_col_df)

        # Drop author_id and in_reply_to_user_id in tweets df, to replace with fields in flattened one-to-one tables).
        tweets = tweets.drop(columns=['author_id', 'in_reply_to_user_id'], errors='ignore')

        # Merge all flattened one-to-one auxiliary dataframes with 'tweets' dataframe
        for i in range(len(aux_df_list)):
            tweets = pd.merge(tweets, aux_df_list[i], on='tweet_id', how='left')

        # Drop original nested columns to create tweets_flat dataframe
        tweets_flat = tweets\
            .drop(columns=[nested_cols_list], errors='ignore')\
            .rename(columns={'like_count': 'public_metrics_like_count',
                             'quote_count': 'public_metrics_quote_count',
                             'reply_count': 'public_metrics_reply_count',
                             'retweet_count': 'public_metrics_retweet_count'})

        # Set reference_level column; tweets_flat now contains reference_level='0' tweets
        tweets_flat['reference_level'] = '0'

        return tweets_flat

    def unpack_referenced_tweets(self, reference_levels_list):
        '''
        Unpacks referenced tweet data from the 'referenced_tweets' column of tweets_flat. Un-nests each set of referenced
        tweets, appends to reference_levels_list and checks for the 'referenced_tweets' column in that 'level'.
        Repeats this process until there are no more levels containing the 'referenced_tweets' column.
        '''

        logging.info('Extracting referenced tweets...')

        for level in reference_levels_list:
            if 'referenced_tweets' in level.columns:
                refd_tweets = level[['tweet_id', 'referenced_tweets']] \
                    .dropna() \
                    .set_index(['tweet_id'])['referenced_tweets'] \
                    .apply(pd.Series) \
                    .stack() \
                    .reset_index()
                refd_tweets_norm = pd.json_normalize(refd_tweets[0])
                referenced_tweets = pd.concat([refd_tweets['tweet_id'], refd_tweets_norm], axis=1)
                TWEETS_LEVEL = referenced_tweets \
                    .reset_index(drop=True) \
                    .rename(columns={'tweet_id': 'referencing_tweet_id', 'id': 'tweet_id'}) \
                    .drop(columns=['author_id', 'in_reply_to_user.id'], errors='ignore')
                # Give each level a reference number
                TWEETS_LEVEL['reference_level'] = str(len(reference_levels_list))

                reference_levels_list.append(TWEETS_LEVEL)

        return reference_levels_list

    def move_referenced_tweet_data_up(self, reference_levels_list, up_a_level_column_list):
        '''
        Copies data from selected columns in tweets_flat, so that they appear in the level at which they are referenced.
        Example: The 'tweet_text' of a level 1 tweet appears as the 'referenced_tweet_text' of a level 0 tweet.
        '''

        # Crete lists to hold values from referenced tweets
        dfs_to_move_up = []
        ref_tweets_up_one_level_list = []
        combined_levels = []

        # Loop through each level in reference_levels_list and move referenced tweet data to same level as referencing tweet
        # dfs_to_move_up[0] match on level.columns will not work but it is discarded anyway
        for level in reference_levels_list:
            for col in up_a_level_column_list:
                if col in level.columns:
                    ref_tweets_up_one_level_list.append(col)
            ref_tweets_up_one_level = level[ref_tweets_up_one_level_list]
            dfs_to_move_up.append(ref_tweets_up_one_level)
            ref_tweets_up_one_level_list = []

        # Rename columns
        for i in range(1, len(dfs_to_move_up)):
            df_to_move = dfs_to_move_up[i].rename(columns=TWEET_fields.dfs_move_up_colnames, errors='ignore')\
                .drop_duplicates(subset=['referenced_tweet_id', 'tweet_id', 'tweet_type'])
            # Combine tweet data with relevant referenced tweet data
            combined_level = pd.merge(reference_levels_list[i - 1], df_to_move, on='tweet_id', how='left')
            # Replace colnames '.' with '_'
            old_cols = combined_level.columns
            new_cols = old_cols.str.replace(".", "_", regex=True).str.replace("__", "", regex=True).tolist()
            cols = dict(zip(old_cols, new_cols))
            combined_level.rename(columns=cols)\
                .sort_index(axis=1)\
                .reset_index(drop=True)
            combined_levels.append(combined_level)

        if len(combined_levels) > 1:
            # Concat everything in combined_levels
            concatted = pd.concat(combined_levels)
            # Put last item in reference_levels_list at bottom
            TWEETS = pd.concat([concatted, reference_levels_list[-1]])
            # Now that 'type' has been moved up a level, rename values in 'tweet_type' column
        else:
            level.columns = level.columns.str.replace(".", "_", regex=True)
            try:
                TWEETS = pd.concat([combined_level, level])
            except:
                TWEETS = level

        if 'tweet_type' in TWEETS.columns:
            TWEETS['tweet_type'] = TWEETS['tweet_type'].replace(
                {'retweeted': 'retweet',
                 'replied_to': 'reply',
                 'quoted': 'quote'})
        else:
            # If no tweet_type column, there are no referenced tweets in the dataset, therefore tweet_type = original
            TWEETS['tweet_type'] = 'original'

        return TWEETS

    def fix_retweet_truncation(self, TWEETS):
        '''
        There is a known issue with the Twitter API where retweet text is truncated to 140 characters. This function uses
        the now-built referenced_tweet columns to extend the tweet_text of retweets, as well as to fill in the hashtags,
        mentions, urls and annotations that are missing from level 0 retweets as a result of this truncation.
        '''

        # If tweet_type == 'retweet', then text = referenced_tweet_text
        if 'referenced_tweet_author_username' and 'referenced_tweet_text' in TWEETS.columns:
            TWEETS['text'] = np.where(TWEETS['tweet_type'] == 'retweet', 'RT @' + TWEETS['referenced_tweet_author_username']
                                      + ': ' + TWEETS['referenced_tweet_text'], TWEETS['text'])

        # If tweet_type = 'retweet', replace original hashtags, mentions, urls and annotations with referenced tweet data
        retweet_fields = ['entities_hashtags', 'entities_mentions', 'entities_urls', 'entities_annotations']
        referenced_fields = ['referenced_tweet_hashtags', 'referenced_tweet_mentions', 'referenced_tweet_urls', 'referenced_tweet_annotations']
        for rt_field, ref_field in zip(retweet_fields, referenced_fields):
            if ref_field in TWEETS.columns:
                if rt_field in TWEETS.columns:
                    TWEETS[rt_field] = np.where(TWEETS['tweet_type'] == 'retweet', TWEETS[ref_field], TWEETS[rt_field])
                else:
                    TWEETS[rt_field] = np.where(TWEETS['tweet_type'] == 'retweet', TWEETS[ref_field], np.nan)

        return TWEETS

    def extract_quote_reply_users(self, TWEETS, URLS):
        '''
        Extracts author usernames where they do not exist with respect to quote tweets and replies; they are retrieved
        from urls and tweet_text.
        '''

        # If tweet_type = 'quote' AND 'referenced_tweet_author_username' = nan, extract quoted tweet usernames from embedded
        # urls and replace nan with extracted username
        if URLS is not None:
            if 'referenced_tweet_author_username' in TWEETS.columns:
                TWEETS = TWEETS.merge(URLS[['tweet_id', 'urls_expanded_url']], how='left', on='tweet_id')
                TWEETS['urls_expanded_url'] = TWEETS['urls_expanded_url'].str.split('/').str[3].replace('@', '')

                TWEETS['referenced_tweet_author_username'] = np.where(TWEETS['tweet_type'] == 'quote',
                                                                      TWEETS['referenced_tweet_author_username'] \
                                                                      .fillna(TWEETS['urls_expanded_url']),
                                                                      TWEETS['referenced_tweet_author_username'])


        # If tweet_type = 'reply' AND 'referenced_tweet_author_username' = nan, extract reply tweet usernames from tweet_text
        # and replace nan with extracted username
        if 'referenced_tweet_author_username' in TWEETS.columns:
            TWEETS['reply_usernames'] = ''
            TWEETS['reply_usernames'] = TWEETS['text'].str.split(' ').str[0]
            TWEETS['reply_usernames'] = TWEETS['reply_usernames'].str.replace('@', '')


            TWEETS['referenced_tweet_author_username'] = np.where(TWEETS['tweet_type'] == 'reply',
                                                                  TWEETS['referenced_tweet_author_username'].fillna(TWEETS['reply_usernames']),
                                                                  TWEETS['referenced_tweet_author_username'])

        return TWEETS

    def process_boolean_cols(self, TWEETS):
        '''
        Process boolean columns.
        '''

        # Add boolean 'is_referenced' column
        TWEETS['is_referenced'] = np.where(TWEETS['reference_level'] == '0', False, True)

        col_to_bool_list = TWEET_fields.col_to_bool_list
        bool_has_list = TWEET_fields.bool_has_list

        # Add boolean has_mention, has_media, has_hashtags, has_urls columns
        for col, bool_col in zip(col_to_bool_list, bool_has_list):
            if col in TWEETS.columns:
                TWEETS[bool_col] = np.where(TWEETS[col] == '', False, True)
            else:
                TWEETS[bool_col] = np.nan

        # Add boolean is_retweet, is_quote, is_reply columns
        if 'tweet_type' in TWEETS.columns:
            TWEETS['is_retweet'] = np.where(TWEETS['tweet_type'] == 'retweet', True, False)
            TWEETS['is_quote'] = np.where(TWEETS['tweet_type'] == 'quote', True, False)
            TWEETS['is_reply'] = np.where(TWEETS['tweet_type'] == 'reply', True, False)
            # Change nan to 'original' in tweet_type column
            TWEETS['tweet_type'] = TWEETS['tweet_type'].fillna('original')
        else:
            TWEETS['is_retweet'] = np.nan
            TWEETS['is_quote'] = np.nan
            TWEETS['is_reply'] = np.nan

        # Set int and bool data types (Google BigQuery will automatically convert date string to TIMESTAMP on upload)
        # Convert boolean columns to boolean arrays (to hold nan values as nan, rather than converting to TRUE)
        boolean_cols = TWEET_fields.boolean_cols
        for bool_col in boolean_cols:
            if bool_col in TWEETS.columns:
                TWEETS[bool_col] = np.where(TWEETS[bool_col].isnull(), pd.NA, np.where(TWEETS[bool_col] == 1., True, TWEETS[bool_col]))
            else:
                TWEETS[bool_col] = pd.NA

        # Convert boolean columns
        TWEETt = TWEETS
        TWEETt[boolean_cols] = TWEETS[boolean_cols].astype('boolean')

        return TWEETS

    def fill_blanks_and_nas(self, TWEETS):
        '''
        Converts converts blanks to nans; converts nans in int and float fields to 0 for consistency and to prevent ValueError.
        '''

        # Convert any blanks to nan for workability
        TWEETS = TWEETS.replace('', np.nan)

        # If column name contains 'public_metrics' (counts), fillna(value=0)
        for col in TWEETS.columns:
            if TWEETS[col].dtype == float:
                TWEETS[col] = TWEETS[col].fillna(value=0)
            if 'public_metrics' in col:
                TWEETS[col] = TWEETS[col].fillna(value=0)

        return TWEETS

class ProcessTables:

    def build_author_description_table(self, TWEETS, entities_mentions):
        '''
        Build author_description table. Table is an amalgamation of author description hashtags, mentions and urls, for
        tweet authors, mentioned authors and in_reply_to authors. Loops through list of author types and dfs from where these
        data are taken e.g. mentioned authors come from entities_mentions; authors come from TWEETS.
        Makes a df for each author type, containing hashtag, mention and url data, then concatenates the three dfs vertically.
        '''
        df_cols_list = [TWEETS, entities_mentions, TWEETS]
        author_desc_dfs = []

        for desc_col, desc_type in zip(df_cols_list, DATA_fields.author_type_list):
            # Description hashtags
            if f'{desc_type}_entities.description.hashtags' in desc_col.columns:
                descr_htags = desc_col[[f'{desc_type}_id', f'{desc_type}_entities.description.hashtags']] \
                    .dropna() \
                    .set_index([f'{desc_type}_id'])[f'{desc_type}_entities.description.hashtags'] \
                    .apply(pd.Series) \
                    .stack() \
                    .reset_index()
                descr_htags_tags = pd.json_normalize(descr_htags[0]) \
                    .add_prefix(f'{desc_type}_description_hashtags_')
                author_descr = pd.concat([descr_htags[f'{desc_type}_id'], descr_htags_tags], axis=1)
            else:
                # Create empty df with starter columns. Mentions and urls are added to this diagonally.
                author_descr = pd.DataFrame(index=[0],
                                            columns=[f'{desc_type}_description{nan_col}' for nan_col in DATA_fields.author_desc_hashtags_cols])

            # Description mentions
            if f'{desc_type}_entities.description.mentions' in desc_col.columns:
                descr_mentions = desc_col[[f'{desc_type}_id', f'{desc_type}_entities.description.mentions']] \
                    .dropna() \
                    .set_index([f'{desc_type}_id'])[f'{desc_type}_entities.description.mentions'] \
                    .apply(pd.Series) \
                    .stack() \
                    .reset_index()
                descr_mentions_mntns = pd.json_normalize(descr_mentions[0]) \
                    .add_prefix(f'{desc_type}_description_mentions_')
                descr_mentions = pd.concat([descr_mentions[f'{desc_type}_id'], descr_mentions_mntns], axis=1)
                author_descr = pd.concat([author_descr, descr_mentions])
            else:
                author_descr = author_descr
                author_descr[[f'{desc_type}_description{nan_col}' for nan_col in DATA_fields.author_desc_mentions_cols]] = np.nan

            # Description urls
            if f'{desc_type}_entities.description.urls' in desc_col.columns:
                descr_urls = desc_col[[f'{desc_type}_id', f'{desc_type}_entities.description.urls']] \
                    .dropna() \
                    .set_index([f'{desc_type}_id'])[f'{desc_type}_entities.description.urls'] \
                    .apply(pd.Series) \
                    .stack() \
                    .reset_index()
                descr_urls_urls = pd.json_normalize(descr_urls[0]) \
                    .add_prefix(f'{desc_type}_description_urls_')
                descr_urls = pd.concat([descr_urls[f'{desc_type}_id'], descr_urls_urls], axis=1)
                author_descr = pd.concat([author_descr, descr_urls])
            else:
                author_descr = author_descr
                author_descr[[f'{desc_type}_description{nan_col}' for nan_col in DATA_fields.author_desc_urls_cols]] = np.nan

            author_descr.columns = author_descr.columns.str.replace(f'{desc_type}', 'author')

            # Append built table to list for concatenation
            author_desc_dfs.append(author_descr)

        # Concatenate tables
        AUTHOR_DESCRIPTION = pd.concat(author_desc_dfs, axis=0)\
            .drop_duplicates()\
            .reset_index(drop=True)\
            .reindex(columns=DATA_fields.author_description_column_order)

        logging.info('AUTHOR_DESCRIPTION table built')

        return AUTHOR_DESCRIPTION

    def build_author_urls_table(self, TWEETS, entities_mentions):
        '''
        Builds table of urls from the profiles of tweet authors, mentioned authors and in_reply_to authors. Similar to
        build_author_description_table, loops through author type and df list to build df.
        '''

        df_cols_list = [TWEETS, entities_mentions, TWEETS]
        author_urls_dfs = []

        for desc_col, desc_type in zip(df_cols_list, DATA_fields.author_type_list):
            # Author urls
            if f'{desc_type}_entities.url.urls' in desc_col.columns:
                author_urls = desc_col[[f'{desc_type}_id', f'{desc_type}_entities.url.urls']].dropna()
                if len(author_urls) > 0:
                    author_urls = author_urls.set_index([f'{desc_type}_id'])[f'{desc_type}_entities.url.urls'] \
                    .apply(pd.Series) \
                    .stack() \
                    .reset_index()
                author_urls_urls = pd.json_normalize(author_urls[0]).add_prefix('author_')
                AUTHOR_URLS = pd.concat([author_urls[f'{desc_type}_id'], author_urls_urls], axis=1) \
                    .reset_index(drop=True) \
                    .drop_duplicates() \
                    .reset_index(drop=True)
            else:
                AUTHOR_URLS = pd.DataFrame(index=[0],
                                           columns=[f'{desc_type}{nan_col}' for nan_col in DATA_fields.author_desc_urls_cols])

            AUTHOR_URLS.columns = AUTHOR_URLS.columns.str.replace(f'{desc_type}', 'author')
            author_urls_dfs.append(AUTHOR_URLS)

        AUTHOR_URLS = pd.concat(author_urls_dfs, axis=0).reindex(columns=DATA_fields.author_urls_column_order)

        logging.info('AUTHOR_URLS table built')

        return AUTHOR_URLS

    def build_media_table(self, TWEETS):
        '''
        Builds media table; a few variant options due to variation in the json format (tweet downloader specifically).
        Extracts column, expands and flattens data. Links to TWEETS table via tweet_id.
        '''

        # If tweet downloader json, rename media column.
        if 'media' in TWEETS.columns:
            TWEETS['attachments_media'] = TWEETS['media']
        if 'attachments_media' in TWEETS.columns:
            media_data = TWEETS[['tweet_id', 'attachments_media']].dropna()
            if len(media_data) > 0:
                media_data = media_data.set_index(['tweet_id'])['attachments_media'] \
                    .apply(pd.Series) \
                    .stack() \
                    .reset_index()
            media_data_media = pd.json_normalize(media_data[0]).add_prefix('media_')
            media_data = pd.concat([media_data['tweet_id'], media_data_media], axis=1) \
                .rename(columns={'media_public_metrics.view_count': 'media_public_metrics_view_count',
                                 'media_media_key': 'media_key'})

            if 'media_public_metrics_view_count' in media_data.columns:
                values = {"media_public_metrics_view_count": 0}
                media_data = media_data.fillna(value=values) \
                    .fillna('null') \
                    .astype({'media_public_metrics_view_count': 'int'})

            if 'media_type' in media_data.columns:
                if 'media_preview_image_url' in media_data.columns:
                    if 'media_url' in media_data.columns:
                        media_data['media_url'] = np.where(media_data['media_type'] != 'photo',
                                                           media_data['media_preview_image_url'],
                                                           media_data['media_url'])
                    else:
                        media_data['media_url'] = media_data['media_preview_image_url']

            MEDIA = media_data.drop(columns=['media_variants'], errors='ignore') \
                .reset_index(drop=True) \
                .reindex(columns=DATA_fields.media_column_order) \
                .drop_duplicates() \
                .reset_index(drop=True)
        else:
            MEDIA = None

        logging.info('MEDIA table built')

        return MEDIA

    def build_poll_options_table(self, TWEETS):
        '''
        Builds poll options table from nested attachments_poll_options column. Extracts column, expands and flattens data.
        Links to TWEETS via 'poll_id' field.
        '''

        if 'attachments_poll_options' in TWEETS.columns:
            poll_options = TWEETS[['attachments_poll_id', 'attachments_poll_options']].dropna()
            if len(poll_options) > 0:
                poll_options = poll_options.set_index(['attachments_poll_id'])['attachments_poll_options'] \
                    .apply(pd.Series) \
                    .stack() \
                    .reset_index()
            poll_options_polls = pd.json_normalize(poll_options[0]).add_prefix('poll_')
            POLL_OPTIONS = pd.concat([poll_options['attachments_poll_id'], poll_options_polls], axis=1) \
                .astype({'poll_position': 'object'}) \
                .rename(columns={'attachments_poll_id': 'poll_id',
                    'attachments_poll_voting_status': 'poll_voting_status',
                    'attachments_poll_duration_minutes': 'poll_duration_minutes',
                    'attachments_poll_end_datetime': 'poll_end_datetime'}) \
                .reset_index(drop=True) \
                .reindex(columns=DATA_fields.poll_options_column_order) \
                .drop_duplicates() \
                .reset_index(drop=True)

        else:
            POLL_OPTIONS = None

        logging.info('POLL_OPTIONS table built')

        return POLL_OPTIONS

    def build_context_annotations_table(self, TWEETS):
        '''
        Builds context_annotations table from nested 'context_annotations' field. Extracts column, expands and flattens
        data. Links to TWEETS table via tweet_id.
        '''

        if 'context_annotations' in TWEETS.columns:
            context_annotations = TWEETS[['tweet_id', 'context_annotations']].dropna()
            if len(context_annotations) > 0:
                context_annotations = context_annotations.set_index(['tweet_id'])['context_annotations'] \
                    .apply(pd.Series) \
                    .stack() \
                    .reset_index()
            context_annotations_ann = pd.json_normalize(context_annotations[0]).add_prefix('tweet_context_annotation_')
            context_annotations = pd.concat([context_annotations['tweet_id'], context_annotations_ann], axis=1)
            context_annotations.columns = context_annotations.columns.str.replace(".", "_", regex=True)
            CONTEXT_ANNOTATIONS = context_annotations \
                .astype({
                'tweet_id': 'string'}) \
                .reset_index(drop=True) \
                .reindex(columns=DATA_fields.context_annotations_column_order) \
                .drop_duplicates() \
                .reset_index(drop=True)
        else:
            CONTEXT_ANNOTATIONS = None

        logging.info('CONTEXT_ANNOTATIONS table built')

        return CONTEXT_ANNOTATIONS

    def build_annotations_table(self, TWEETS):
        '''
        Builds annotations table from nested 'entities_annotations' field. Extracts column, expands and flattens data. Links to
        TWEETS table via tweet_id.
        '''

        if 'entities_annotations' in TWEETS.columns:
            entities_annotations = TWEETS[['tweet_id', 'entities_annotations']].dropna()
            if len(entities_annotations) > 0:
                entities_annotations = entities_annotations.set_index(['tweet_id'])['entities_annotations'] \
                    .apply(pd.Series) \
                    .stack() \
                    .reset_index()
                entities_annotations_ann = pd.json_normalize(entities_annotations[0]).add_prefix('tweet_annotation_')
                ANNOTATIONS = pd.concat([entities_annotations['tweet_id'], entities_annotations_ann], axis=1) \
                    .reset_index(drop=True) \
                    .reindex(columns=DATA_fields.annotations_column_order) \
                    .drop_duplicates() \
                    .reset_index(drop=True)
            else:
                ANNOTATIONS = None
        else:
            ANNOTATIONS = None

        logging.info('ANNOTATIONS table built')

        return ANNOTATIONS

    def build_hashtags_table(self, TWEETS):
        '''
        Builds hashtags table from nested 'entities_hashtags' field. Extracts column, expands and flattens
        data. Links to TWEETS table via tweet_id.
        '''

        if 'entities_hashtags' in TWEETS.columns:
            entities_hashtags = TWEETS[['tweet_id', 'entities_hashtags']].dropna()
            if len(entities_hashtags) > 0:
                entities_hashtags = entities_hashtags.set_index(['tweet_id'])['entities_hashtags'] \
                    .apply(pd.Series) \
                    .stack() \
                    .reset_index()
                entities_hashtags_tag = pd.json_normalize(entities_hashtags[0]).add_prefix('hashtags_')
                if 'hashtags_text' in entities_hashtags_tag:
                    entities_hashtags_tag = entities_hashtags_tag.rename(columns={'hashtags_text':'hashtags_tag'})

                HASHTAGS = pd.concat([entities_hashtags['tweet_id'], entities_hashtags_tag], axis=1) \
                    .dropna(subset='hashtags_tag') \
                    .reset_index(drop=True) \
                    .reindex(columns=DATA_fields.hashtags_column_order) \
                    .drop_duplicates() \
                    .reset_index(drop=True)
            else:
                HASHTAGS = None
        else:
            HASHTAGS = None

        logging.info('HASHTAGS table built')

        return HASHTAGS

    def extract_entities_data(self, TWEETS):
        '''
        Pulls 'entities_mentions' data from TWEETS table; used to build other tables from nested fields in TWEETS. Extracts
        column, expands and flattens data.
        '''

        entities_mentions = TWEETS[['tweet_id', 'entities_mentions']] \
            .dropna() \
            .set_index(['tweet_id'])['entities_mentions'] \
            .apply(pd.Series) \
            .stack() \
            .reset_index()
        entities_mentions_mntns = pd.json_normalize(entities_mentions[0]) \
            .add_prefix('tweet_mentions_author_')
        entities_mentions = pd.concat([entities_mentions['tweet_id'], entities_mentions_mntns], axis=1) \
            .replace('', np.nan)
        # Replace nans in int fields with 0
        for column in entities_mentions.columns:
            if entities_mentions[column].dtype == float:
                entities_mentions[column] = entities_mentions[column].fillna(value=0)
                entities_mentions[column] = entities_mentions[column].astype('int32')

        return entities_mentions

    def build_mentions_table(self, entities_mentions):
        '''
        Builds MENTIONS table from 'entities_mentions' table. Extracts column, expands and flattens
        data. Links to TWEETS table via tweet_id.
        '''

        if len(entities_mentions) > 0:
            MENTIONS = entities_mentions \
                .reset_index(drop=True) \
                .reindex(columns=DATA_fields.mention_column_order) \
                .drop_duplicates()
        else:
            MENTIONS = None

        logging.info('MENTIONS table built')

        return MENTIONS

    def build_urls_table(self, TWEETS):
        '''
        Builds urls table from nested 'entities_urls' field. Extracts column, expands and flattens
        data. Links to TWEETS table via tweet_id.
        '''

        if 'entities_urls' in TWEETS.columns:
            entities_urls = TWEETS[['tweet_id', 'entities_urls']].dropna()
            if len(entities_urls) > 0:
                entities_urls = entities_urls.set_index(['tweet_id'])['entities_urls'] \
                    .apply(pd.Series) \
                    .stack() \
                    .reset_index()
                entities_urls_urls = pd.json_normalize(entities_urls[0]) \
                    .add_prefix('urls_')
                # TODO: DEAL WITH URLS_IMAGES - put in media table?
                if 'urls_images' in entities_urls_urls.columns:
                    entities_urls_urls = entities_urls_urls.drop(columns=['urls_images'])
                URLS = pd.concat([entities_urls['tweet_id'], entities_urls_urls], axis=1) \
                    .reset_index(drop=True) \
                    .reindex(columns=DATA_fields.urls_column_order) \
                    .drop_duplicates()
            else:
                URLS = None
        else:
            URLS = None
        logging.info('URLS table built')

        return URLS

    def build_interactions_table(self, TWEETS, MENTIONS):
        '''
        Uses mentions and tweet data to generate a new table with all mentions (includes replies, quotes and retweets).
        This is required because 'mentions' does not include users that have been replied to, quoted or retweeted. This
        table is constructed using tweet_type and refrenced_tweet_author data to provide a more comprehensive table of
        interactions.
        '''

        if 'referenced_tweet_author_id' in TWEETS.columns:
            # First build interactions from TWEETS table
            interactions = TWEETS[['tweet_id', 'tweet_type', 'referenced_tweet_author_id', 'referenced_tweet_author_username']] \
                .rename(columns={
                    'referenced_tweet_author_id': 'to_user_id',
                    'referenced_tweet_author_username': 'to_user_username'})
        else:
            interactions = pd.DataFrame(columns={'tweet_id', 'tweet_type', 'to_user_id', 'to_user_username'})

        # Next, build interactions_mentions from MENTIONS table
        if MENTIONS is not None:
            interactions_mentions = MENTIONS[['tweet_id', 'tweet_mentions_author_id', 'tweet_mentions_author_username']] \
                .rename(columns={'tweet_mentions_author_id': 'to_user_id', 'tweet_mentions_author_username': 'to_user_username'})
            interactions_mentions['tweet_type'] = 'mention'
            interactions = interactions \
                .drop(interactions.index[interactions['tweet_type'] == 'original'])
        else:
            interactions_mentions = pd.DataFrame(columns={'tweet_id', 'to_user_id', 'to_user_username'})

        # Concatenate interactions and interactions_mentions
        INTERACTIONS = pd.concat([interactions, interactions_mentions], ignore_index=True)\
            .reset_index(drop=True) \
            .dropna(subset='tweet_type') \
            .reindex(columns=DATA_fields.interactions_column_order) \
            .drop_duplicates()

        logging.info('INTERACTIONS table built')

        return INTERACTIONS

    def build_edit_history_table(self, TWEETS):
        '''
        Builds urls table from 'edit_history_tweet_ids' list field. Extracts column, expands and flattens
        data. Links to TWEETS table via tweet_id.
        '''

        if 'edit_history_tweet_ids' in TWEETS.columns:
            edit_history_tweet_ids = TWEETS[['tweet_id', 'edit_history_tweet_ids']].dropna()
            if len(edit_history_tweet_ids) > 0:
                edit_history_tweet_ids = edit_history_tweet_ids.set_index(['tweet_id'])['edit_history_tweet_ids'] \
                    .apply(pd.Series) \
                    .stack() \
                    .reset_index()
                EDIT_HISTORY = edit_history_tweet_ids[['tweet_id', 0]]\
                    .reset_index(drop=True) \
                    .rename(columns={0:'edit_history_tweet_ids'}) \
                    .reindex(columns=DATA_fields.edit_history_column_order) \
                    .drop_duplicates()
            else:
                EDIT_HISTORY = None
        else:
            EDIT_HISTORY = None

        logging.info('EDIT_HISTORY table built')

        return EDIT_HISTORY

class SchemaTransform:

    def transform_DATA_to_TCAT(self, list_of_dataframes):
        '''
        Transforms tables in DATA format for compatibility with existing TCAT datasets. Selects relevant fields from TWEETS,
        HASHTAGS, MENTIONS and URLS and renames fields that occur in the TCAT schema. TCAT fields for which DATA has no
        equivalent field are given blank values.
        '''
        # todo URLS table

        # TWEETS
        TWEETS = list_of_dataframes[0].copy()
        # Rename and reformat miscellaneous Tweets columns
        TWEETS['time'] = TWEETS['created_at']
        # Format 'author_created_at'
        TWEETS['author_created_at'] = pd.to_datetime(TWEETS["author_created_at"], format="%Y-%m-%dT%H:%M:%S")
        # Use 'referenced_tweet_id' to create columns for quoted and in_reply_to status ids
        TWEETS['in_reply_to_status_id'] = TWEETS['referenced_tweet_id']
        TWEETS['quoted_status_id'] = TWEETS['referenced_tweet_id']
        # Then make the columns blank if tweet_type does not match
        TWEETS.loc[TWEETS['tweet_type'] != 'quote', 'quoted_status_id'] = ''
        TWEETS.loc[TWEETS['tweet_type'] != 'reply', 'in_reply_to_status_id'] = ''
        # Create blank columns (that don't have an equivalent in the DATA schema)
        TWEETS[[f'{blank_col}' for blank_col in TCAT_fields.blank_cols]] = ''
        TWEETS = TWEETS[TCAT_fields.tweet_column_order].rename(columns=TCAT_fields.tweet_column_names_dict)

        # HASHTAGS
        if list_of_dataframes[4] is not None:
            HASHTAGS = list_of_dataframes[4].copy()
            # Get hashtags data; rename columns
            HASHTAGS = HASHTAGS[TCAT_fields.hashtags_column_order].rename(columns=TCAT_fields.hashtags_column_names_dict)
        else:
            HASHTAGS = pd.DataFrame(columns=[TCAT_fields.hashtags_column_order])

        # MENTIONS
        if list_of_dataframes[10] is not None:
            INTERACTIONS = list_of_dataframes[10].copy()
            # Get mentions data from INTERACTIONS and TWEETS tables
            interactions_to = INTERACTIONS.dropna().rename(columns=TCAT_fields.interactions_to_column_names_dict)
            interactions_from = TWEETS[['id', 'from_user_id', 'from_user_name']].rename(columns=TCAT_fields.interactions_from_column_names_dict)
            MENTIONS = interactions_to.merge(interactions_from, how='left', on='id')\
                .drop_duplicates()\
                .rename(columns=TCAT_fields.mentions_column_names_dict)
            MENTIONS = MENTIONS[TCAT_fields.MENTIONS_column_order]
        else:
            MENTIONS = pd.DataFrame(columns=[TCAT_fields.MENTIONS_column_order])

        list_of_dataframes = [TWEETS, HASHTAGS, MENTIONS]

        return list_of_dataframes

    def transform_DATA_to_TQ(self, list_of_dataframes):
        '''
        Transforms tables in DATA format for compatibility with existing TweetQuery datasets. Selects relevant fields from TWEETS,
        HASHTAGS, MENTIONS, INTERACTIONS and URLS and renames fields that occur in the TweetQuery schema. TweetQuery fields for which DATA has no
        equivalent field are given blank values.
        '''

        # TWEETS
        TWEETS = list_of_dataframes[0].copy()
        # Generate 'in_reply_to' fields from 'referenced_tweet' fields
        TWEETS['in_reply_to_screen_name'] = TWEETS['referenced_tweet_author_username']
        TWEETS['in_reply_to_status_id'] = TWEETS['referenced_tweet_id']
        TWEETS['in_reply_to_user_id'] = TWEETS['referenced_tweet_author_id']
        # Generate 'quoted' fields from 'referenced_tweet' fields
        TWEETS['is_quote_status'] = np.where(TWEETS['tweet_type'] == 'quote', True, False)
        TWEETS['quoted_status_id'] = TWEETS['referenced_tweet_id']
        TWEETS['quoted_status_text'] = TWEETS['referenced_tweet_text']
        TWEETS['quoted_status_user_id'] = TWEETS['referenced_tweet_author_id']
        # Generate 'retweeted' fields from 'referenced_tweet' fields
        TWEETS['retweeted'] = np.where(TWEETS['tweet_type'] == 'retweet', True, False)
        TWEETS.loc[TWEETS['tweet_type'] == 'retweet', 'retweeted'] = 'true'
        TWEETS['retweeted_status_id'] = TWEETS['referenced_tweet_id']
        TWEETS['retweeted_status_user_id'] = TWEETS['referenced_tweet_author_id']

        # Keep values in new fields above only if 'tweet_type' matches
        # todo can these be collapsed? a la [['quoted_status_id', 'quoted_status_text', 'quoted_status_user_id']]
        TWEETS.loc[TWEETS['tweet_type'] != 'quote', 'quoted_status_id'] = ''
        TWEETS.loc[TWEETS['tweet_type'] != 'quote', 'quoted_status_text'] = ''
        TWEETS.loc[TWEETS['tweet_type'] != 'quote', 'quoted_status_user_id'] = ''

        TWEETS.loc[TWEETS['tweet_type'] != 'reply', 'in_reply_to_status_id'] = ''
        TWEETS.loc[TWEETS['tweet_type'] != 'reply', 'in_reply_to_user_id'] = ''
        TWEETS.loc[TWEETS['tweet_type'] != 'reply', 'in_reply_to_screen_name'] = ''

        TWEETS.loc[TWEETS['tweet_type'] != 'retweet', 'retweeted_status_id'] = ''
        TWEETS.loc[TWEETS['tweet_type'] != 'retweet', 'retweeted_status_user_id'] = ''

        # Misc columns
        TWEETS['user_profile_image_url_https'] = TWEETS['author_profile_image_url'].replace('http', 'https')
        TWEETS['author_created_at'] = pd.to_datetime(TWEETS["author_created_at"], format="%Y-%m-%dT%H:%M:%S")
        # Create blank columns (that don't have an equivalent in the DATA schema)
        TWEETS[[f'{blank_col}' for blank_col in TweetQuery_fields.blank_cols]] = ''

        # HASHTAGS
        if list_of_dataframes[4] is not None:
            HASHTAGS = list_of_dataframes[4].copy()
            HASHTAGS = HASHTAGS[TweetQuery_fields.hashtags_column_order]\
                .rename(columns=TweetQuery_fields.hashtags_column_names_dict)
            TWEETS = TWEETS.merge(HASHTAGS, how='left', on='tweet_id')
        else:
            HASHTAGS = pd.DataFrame(columns=[TweetQuery_fields.hashtags_column_order])
            TWEETS = TWEETS.merge(HASHTAGS, how='left', on='tweet_id')

        # MENTIONS
        if list_of_dataframes[6] is not None:
            # Get mentioned author names from MENTIONS table and set aside
            mentioned_author_names = list_of_dataframes[6][['tweet_mentions_author_id', 'tweet_mentions_author_name']]
        else:
            # If None, then create empty dataframe
            mentioned_author_names = pd.DataFrame(columns=['tweet_mentions_author_id', 'tweet_mentions_author_name'])
        if list_of_dataframes[10] is not None:
            INTERACTIONS = list_of_dataframes[10].copy()
            # Get referenced author names from TWEETS table and set aside
            referenced_author_names = TWEETS[['referenced_tweet_author_id', 'referenced_tweet_author_name']]\
                .rename(columns={'referenced_tweet_author_id':'tweet_mentions_author_id',
                                 'referenced_tweet_author_name': 'tweet_mentions_author_name'})
            # Concatenate mentioned_author and referenced_tweet_author names
            mentioned_author_names_concat = pd.concat([mentioned_author_names, referenced_author_names], axis=0)\
                .rename(columns={'tweet_mentions_author_id':'to_user_id'})
            # Merge author names with INTERACTIONS, rename fields to be compatible with TQ schema
            interactions_to = INTERACTIONS.merge(mentioned_author_names_concat, how='left', on='to_user_id').drop_duplicates()
            interactions_to = interactions_to.dropna().rename(columns=TweetQuery_fields.interactions_to_column_names_dict)
            # Reorder and create MENTIONS table
            MENTIONS = interactions_to[TweetQuery_fields.MENTIONS_column_order]
            # Merge MENTIONS table with TWEETS
            TWEETS = TWEETS.merge(MENTIONS, how='left', on='tweet_id')
        else:
            MENTIONS = pd.DataFrame(columns=[TweetQuery_fields.MENTIONS_column_order])
            TWEETS = TWEETS.merge(MENTIONS, how='left', on='tweet_id')

        # URLs
        if list_of_dataframes[5] is not None:
            URLS = list_of_dataframes[5].copy()
            URLS = URLS.astype(str)
            URLS['urls_unshortened_url'] = URLS['urls_unwound_url']
            URLS['urls_domain_path'] = URLS['urls_unwound_url'].str.split('/').str[0:3]
            URLS['urls_domain_path'] = ['/'.join(map(str, l)) for l in URLS['urls_domain_path']]
            URLS = URLS[TweetQuery_fields.urls_column_order]
            TWEETS = TWEETS.merge(URLS, how='left', on='tweet_id')
        else:
            URLS = pd.DataFrame(columns=[TweetQuery_fields.urls_column_order])
            TWEETS = TWEETS.merge(URLS, how='left', on='tweet_id')


        TWEETS = TWEETS[TweetQuery_fields.tweet_column_order]\
            .rename(columns=TweetQuery_fields.tweet_column_names_dict)

        list_of_dataframes = [TWEETS]

        return list_of_dataframes


# MAIN
# ----------------------------------------------------------------------------------------------------------------------

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
            archive_search_counts, readable_time_estimate = get_pre_search_counts(client, query, start_date, end_date) #TODO *args??
            interval, num_intervals = calculate_interval(start_date, end_date, archive_search_counts)
            # TODO if archive_search_counts == 0, do nut run search...
            # Print search results for user and ask to proceed
            print(f"""
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
                    to_collect, expected_files = set_up_expected_files(start_date, end_date, json_filepath, option_selection, dataset, interval)
                    # Call function collect_archive_data()
                    collect_archive_data(bq, project, dataset, to_collect, expected_files, client, query, start_date, end_date, csv_filepath, archive_search_counts, tweet_count, query, query_count, schematype)
                    # Notify user of completion
                    notify_completion(bq, search_start_time, project, dataset, start_date, end_date, option_selection, archive_search_counts, subquery=query, interval=interval)

                except Exception as error:
                    traceback_info = capture_error_string(error, error_filepath)
                    send_error_email(mailgun_domain, mailgun_key, dataset, traceback_info)
                    logging.info('Error email sent to admin.')
            else:
                exit()

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
                        # to_collect, expected files tell the program what to collect and what has already been collected
                        to_collect, expected_files = set_up_expected_files(start_date, end_date, json_filepath, option_selection, subquery, interval)

                        subquery = f'@{subquery} OR from:{subquery}'

                        # Pre-search archive counts
                        logging.info('-----------------------------------------------------------------------------------------')
                        logging.info(f'Getting tweet counts for query: {subquery}')
                        archive_search_counts, readable_time_estimate = get_pre_search_counts(client, subquery, start_date, end_date)
                        # TODO if archive_search_counts == 0, do nut run search...
                        logging.info(f'{archive_search_counts} tweets for query: {subquery}')

                        # Call function collect_archive_data()
                        collect_archive_data(bq, project, dataset, to_collect, expected_files, client, subquery, start_date, end_date, csv_filepath, archive_search_counts, tweet_count, query, query_count, schematype)
                        # Notify user of completion
                        notify_completion(bq, search_start_time, project, dataset, start_date, end_date, option_selection, archive_search_counts, subquery=subquery, interval=interval)

                except Exception as error:
                    traceback_info = capture_error_string(error, error_filepath)
                    send_error_email(mailgun_domain, mailgun_key, dataset, traceback_info)
                    logging.info('Error email sent to admin.')
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
                    tweet_count = process_json_data(a_file, csv_filepath, bq, project, dataset, query, start_date, end_date, archive_search_counts, tweet_count, schematype)
                    # Notify user of completion
                    notify_completion(bq, search_start_time, project, dataset, start_date, end_date, option_selection, archive_search_counts, subquery=query, interval=0)
            else:
                exit()
        else:
            print('Invalid selection')

    except Exception as error:
        traceback_info = capture_error_string(error, error_filepath)
        send_error_email(mailgun_domain, mailgun_key, dataset, traceback_info)
        logging.info('Error email sent to admin.')
