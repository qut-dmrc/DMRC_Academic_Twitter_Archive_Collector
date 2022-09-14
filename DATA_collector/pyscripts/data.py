import json
import pandas as pd
import numpy as np
import time
from time import sleep
import humanfriendly
from humanfriendly import format_timespan
import traceback
import re
import pytz

from twarc import Twarc2, expansions
from google.cloud import bigquery
from google.cloud.bigquery.client import Client
from google.cloud.exceptions import NotFound

from .bq_schema import DATA_schema, TCAT_schema
from .emails import *
from .fields import DATA_fields, TCAT_fields
from .set_up_directories import *

pd.options.mode.chained_assignment = None
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)



# ----------------------------------------------------------------------------------------------------------------------

# Functions
# ---------
def validate_search_parameters(query, bearer_token, start_date, end_date, project, dataset, interval):

    utc = pytz.UTC

    print("""
    Validating your config...
    """)

    # Search query length
    if len(query) in range(1, 1024):
        query = query
    elif len(query) < 1:
        print('Please enter a search query.')
        query = None
    else:
        query = None
        print(f'Query is too long ({len(query)} characters). Please shorten it to 1024 characters or less and retry.')

    # Bearer token entered
    if len(bearer_token) > 0:
        bearer_token = bearer_token
    else:
        bearer_token = None
        print('Please enter a valid bearer token for Twitter API access.')

    # Google access key entered
    if glob.glob(f'{cwd}/access_key/*.json'):
        access_key = glob.glob(f'{cwd}/access_key/*.json')
    else:
        access_key = None
        print('Please enter a valid Google service account access key')

    # Start date in the past
    if start_date < end_date:
        start_date = start_date
    elif start_date > utc.localize(dt.datetime.now()):
        start_date = None
        print('Start date cannot be in the future.')
    else:
        start_date = None
        print('Start date cannot be after end date.')

    # End date in the past, but after start date
    if end_date < utc.localize(dt.datetime.now()):
        end_date = end_date
    else:
        end_date = None
        print('End date cannot be in the future.')

    # Project name entered
    if len(project) > 0:
        project = project
    else:
        project = None
        print('Please enter a Google BigQuery billing project.')

    # Dataset name entered
    if len(dataset) > 0:
        dataset = dataset
    else:
        dataset = None
        print('Please enter a name for your desired dataset.')

    # Interval
    if interval != '0':
        interval = interval
    else:
        interval = 1

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

        return query, bearer_token, access_key, start_date, end_date, project, dataset, interval

def get_pre_search_counts(client, query, start_date, end_date, project, dataset, schematype, interval):
    # Run counts_all search
    count_tweets = client.counts_all(query=query, start_time=start_date, end_time=end_date)

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

    time_estimate = (archive_search_counts*0.0012379*60)
    readable_time_estimate = format_timespan(time_estimate)

    print(f"""
    Please check the below details carefully, and ensure you have enough room in your bearer token quota!
    \n
    Your query: {query}
    Start date: {start_date}
    End date: {end_date}
    Destination database: {project}.{dataset}
    Schema type: {schematype}
    Intervals (days): {interval}
    \n
    Your archive search will collect approximately {archive_search_counts} tweets (upper estimate).
    This could take around {readable_time_estimate}.
    \n 
    \n
    Proceed? y/n""")

    user_proceed = input('>>>')

    return archive_search_counts, user_proceed

def collect_archive_data(bq, project, dataset, to_collect, expected_files, client, query, start_date, end_date, csv_filepath, archive_search_counts, tweet_count):
    logging.info('Commencing data collection...')

    # Collect archive data one interval at a time using the Twarc search_all endpoint
    if len(to_collect) > 0:
        for a_file in to_collect:
            logging.info('-----------------------------------------------------------------------------------------')
            logging.info(f'Collecting file {a_file}')
            start, end = expected_files[a_file]
            search_results = client.search_all(query=query, start_time=start, end_time=end, max_results=100)

            for page in search_results:
                result = expansions.flatten(page)
                for tweet in result:
                    json_object = (json.dumps(tweet))
                    with open(a_file, "a") as f:
                        f.write(json_object + "\n")

            tweetsList = []
            if os.path.isfile(a_file):
                with open(a_file) as f:
                    for jsonObj in f:
                        tweetDict = json.loads(jsonObj)
                        tweetsList.append(tweetDict)

                    logging.info(f'Processing tweet data...')

                    # For each interval (file), process json
                    tweet_count = process_json_data(a_file, csv_filepath, bq, project, dataset, query, start_date, end_date, archive_search_counts, tweet_count)



def process_json_data(a_file, csv_filepath, bq, project, dataset, query, start_date, end_date, archive_search_counts, tweet_count):
    for chunk in pd.read_json(a_file, lines=True, dtype=False, chunksize=10000):
        tweets = chunk

        # Rename 'id' field for clarity
        tweets = tweets.rename(columns={'id': 'tweet_id'})
        tweets['tweet_id'] = tweets['tweet_id'].astype(object)

        # Call function to flatten top level tweets and merge with flattened columns
        tweets_flat = flatten_top_tweet_level(tweets)

        # Start reference_levels_list with tweets_flat and append lower reference levels
        reference_levels_list = [tweets_flat]
        reference_levels_list = unpack_referenced_tweets(reference_levels_list)

        up_a_level_column_list = DATA_fields.up_a_level_column_list
        # Move referenced tweet data from reference levels up a level as 'referenced_tweet' data
        TWEETS = move_referenced_tweet_data_up(reference_levels_list, up_a_level_column_list)
        logging.info('TWEETS table built')

         # ONE-TO-MANY NESTED COLUMNS
        # --------------------------
        # Save nested columns as new expanded dataframes with corresponding tweet_id
        logging.info('Unpacking one-to-many nested columns...')

        if 'entities_mentions' in TWEETS.columns:
            entities_mentions = extract_entities_data(TWEETS)

        if Schematype.DATA == True:
            TWEETS = TWEETS
            MENTIONS = build_mentions_table(entities_mentions)
            AUTHOR_DESCRIPTION = build_author_description_table(TWEETS, entities_mentions)
            AUTHOR_URLS = build_author_urls_table(TWEETS, entities_mentions)
            MEDIA = build_media_table(TWEETS)
            POLL_OPTIONS = build_poll_options_table(TWEETS)
            CONTEXT_ANNOTATIONS = build_context_annotations_table(TWEETS)
            ANNOTATIONS = build_annotations_table(TWEETS)
            HASHTAGS = build_hashtags_table(TWEETS)
            URLS = build_urls_table(TWEETS)
            INTERACTIONS = build_interactions_table(TWEETS, URLS, MENTIONS)
        else:
            TWEETS = TWEETS.loc[TWEETS['reference_level'] == '0']
            MENTIONS = build_mentions_table(entities_mentions)
            AUTHOR_DESCRIPTION = None
            AUTHOR_URLS = None
            MEDIA = None
            POLL_OPTIONS = None
            CONTEXT_ANNOTATIONS = None
            ANNOTATIONS = None
            HASHTAGS = build_hashtags_table(TWEETS)
            URLS = None
            INTERACTIONS = None

        # Special case of geo_geo_bbox: convert from column of lists to strings
        if 'geo_geo_bbox' in TWEETS.columns:
            TWEETS = TWEETS.fillna('')
            TWEETS['geo_geo_bbox'] = [','.join(map(str, l)) for l in TWEETS['geo_geo_bbox']]

        # BACK TO TWEETS DATAFRAME
        # ------------------------
        # Convert any blanks to nan for workability
        TWEETS = TWEETS.replace('', np.nan)

        # If column name contains 'count', fillna(value=0)
        for column in TWEETS.columns:
            if TWEETS[column].dtype == float:
                TWEETS[column] = TWEETS[column].fillna(value=0)

        # BOOLEAN COLUMNS
        # ---------------
        # Add boolean 'is_referenced' column
        TWEETS['is_referenced'] = np.where(TWEETS['reference_level'] == '0', False, True)

        col_to_bool_list = DATA_fields.col_to_bool_list
        bool_has_list = DATA_fields.bool_has_list
        # Add boolean has_mention, has_media, has_hashtags, has_urls columns
        for col, bool_col in zip(col_to_bool_list, bool_has_list):
            if col in TWEETS.columns:
                TWEETS[bool_col] = np.where(~TWEETS[col].isnull(), True, False)
            else:
                TWEETS[bool_col] = np.nan

        # Add boolean is_retweet, is_quote, is_reply columns
        if 'tweet_type' in TWEETS.columns:
            TWEETS['is_retweet'] = np.where(TWEETS['tweet_type'] == 'retweet', True, False)
            TWEETS['is_quote'] = np.where(TWEETS['tweet_type'] == 'quote', True, False)
            TWEETS['is_reply'] = np.where(TWEETS['tweet_type'] == 'reply', True, False)
            # change nan to 'original' in tweet_type column
            TWEETS['tweet_type'] = TWEETS['tweet_type'].fillna('original')
        else:
            TWEETS['is_retweet'] = np.nan
            TWEETS['is_quote'] = np.nan
            TWEETS['is_reply'] = np.nan

        # Set int and bool data types (BigQeury will automatically convert string to TIMESTAMP on upload)
        # convert boolean columns to boolean arrays (to hold nan values as nan, rather than converting to TRUE)
        boolean_cols = DATA_fields.boolean_cols
        for bool_col in boolean_cols:
            if bool_col in TWEETS.columns:
                TWEETS[bool_col] = np.where(TWEETS[bool_col].isnull(), pd.NA,
                                                np.where(TWEETS[bool_col] == 1., True, TWEETS[bool_col]))

        TWEETS = TWEETS.rename(columns={
            'text': 'tweet_text',
            'attachments_poll_id': 'poll_id',
            'attachments_poll_voting_status': 'poll_voting_status',
            'attachments_poll_duration_minutes': 'poll_duration_minutes',
            'attachments_poll_end_datetime': 'poll_end_datetime'})

        # FINALISE FIELDS AND DATA TYPES IN TWEETS TABLE
        # ----------------------------------------------
        TWEETS = TWEETS \
            .reset_index(drop=True) \
            .reindex(columns=DATA_fields.tweet_column_order) \
            .drop_duplicates()
        # Convert boolean columns
        TWEETS[boolean_cols] = TWEETS[boolean_cols].astype('boolean')
        # Convert the rest of the data types
        for col in TWEETS.columns:
            if 'public_metrics' in col:
                TWEETS[col] = TWEETS[col].fillna(value=0)
        # Convert the rest of the data types
        TWEETS = TWEETS.astype(DATA_fields.tweet_table_dtype_dict)

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
                              INTERACTIONS]

        # Proceed according to schema chosen
        # ----------------------------------
        list_of_dataframes, list_of_csv, list_of_tablenames, list_of_schema, tweet_count = get_schema_type(list_of_dataframes, tweet_count)


        # For each processed json, write to temp csv
        for tweetframe, csv_file in zip(list_of_dataframes, list_of_csv):
            write_processed_data_to_csv(tweetframe, csv_file, csv_filepath)

        percent_collected = tweet_count / archive_search_counts * 100
        logging.info(f'{tweet_count} of {archive_search_counts} tweets collected ({round(percent_collected, 1)}%)')

        # Write temp csv files to BigQuery tables
        push_processed_tables_to_bq(bq, project, dataset, list_of_tablenames, csv_filepath, list_of_csv, query, start_date, end_date, list_of_schema, list_of_dataframes)

    return tweet_count

def flatten_top_tweet_level(tweets):
    logging.info('Flattening one-to-one nested columns...')

    nested_cols_list = ['entities',
                        'public_metrics',
                        'author',
                        'in_reply_to_user',
                        'attachments',
                        'geo',
                        '__twarc']

    # Create empty list to hold auxiliary dataframes for merging with main tweet dataframe
    aux_df_list = []

    # Isolate each nested column, flatten, save to dataframe and append to aux_df_list
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

    # Drop columns that contain duplicates in auxiliary dataframes
    tweets = tweets.drop(columns=['author_id', 'in_reply_to_user_id'], errors='ignore')

    # Merge all flattened auxiliary dataframes with 'tweets' dataframe
    for i in range(len(aux_df_list)):
        tweets = pd.merge(tweets, aux_df_list[i], on='tweet_id', how='left')

    # Drop original nested columns to create tweets_flat dataframe
    tweets_flat = tweets\
        .drop(columns=[nested_cols_list], errors='ignore')\
        .rename(columns={'like_count': 'public_metrics_like_count',
                         'quote_count': 'public_metrics_quote_count',
                         'reply_count': 'public_metrics_reply_count',
                         'retweet_count': 'public_metrics_retweet_count'})

    # Set reference_level column
    tweets_flat['reference_level'] = '0'

    return tweets_flat

def unpack_referenced_tweets(reference_levels_list):
    logging.info('Extracting referenced tweets...')

    for level in reference_levels_list:
        if 'referenced_tweets' in level.columns:
            # Extract referenced_tweets from TWEETS_LV0
            referenced_tweets = level[['tweet_id', 'referenced_tweets']].dropna().reset_index(drop=True)
            # Create series containing tweet ids, and convert to dataframe
            referenced_tweets_id = referenced_tweets['tweet_id'].to_frame()
            # Create series containing nested referenced tweet data and flatten to one nested column
            # per referenced tweet
            referenced_tweets = pd.json_normalize(referenced_tweets['referenced_tweets'])
            # Calculate the number of times tweet ids should be repeated, based on maximum number of
            # referenced tweets (n columns)
            num_repeats = len(referenced_tweets.columns)
            referenced_tweets_id = pd.concat([referenced_tweets_id] * num_repeats).reset_index(drop=True)
            # Stack the nested referenced tweet columns vertically (melt),
            # then concatenate with stacked tweet ids to get matching ids and referenced tweet data
            referenced_tweets = referenced_tweets.melt().reset_index(drop=True)
            referenced_tweets = pd.concat([referenced_tweets_id, referenced_tweets], axis=1)
            # Finally, flatten the 'value' column (containing the stacked, nested referenced tweets
            TWEETS_LV_TEMP = pd.json_normalize(referenced_tweets['value'])
            TWEETS_LV_TEMP = pd.concat([referenced_tweets_id, TWEETS_LV_TEMP], axis=1).dropna(subset=['id'])\
                .reset_index(drop=True)\
                .rename(columns={'tweet_id': 'referencing_tweet_id', 'id': 'tweet_id'})\
                .drop(columns=['author_id', 'in_reply_to_user.id'], errors='ignore')
            TWEETS_LEVEL = TWEETS_LV_TEMP
            TWEETS_LEVEL['reference_level'] = str(len(reference_levels_list))
            reference_levels_list.append(TWEETS_LEVEL)

    return reference_levels_list

def move_referenced_tweet_data_up(reference_levels_list, up_a_level_column_list):
    # Crete lists to hold values from referenced tweets
    dfs_to_move_up = []
    ref_tweets_up_one_level_list = []
    combined_levels = []

    # Loop through each level and move referenced tweet data up a level
    for level in reference_levels_list:
        for col in up_a_level_column_list:
            if col in level.columns:
                ref_tweets_up_one_level_list.append(col)
        ref_tweets_up_one_level = level[ref_tweets_up_one_level_list]
        dfs_to_move_up.append(ref_tweets_up_one_level)
        ref_tweets_up_one_level_list = []

    # Rename columns
    for i in range(1, len(dfs_to_move_up)):
        df_to_move = dfs_to_move_up[i].rename(columns={
            'tweet_id': 'referenced_tweet_id',
            'referencing_tweet_id': 'tweet_id',
            'type': 'tweet_type',
            'text': 'referenced_tweet_text',
            'entities.hashtags': 'referenced_tweet_hashtags',
            'entities.mentions': 'referenced_tweet_mentions',
            'entities.urls': 'referenced_tweet_urls',
            'entities.annotations': 'referenced_tweet_annotations',
            'author.id': 'referenced_tweet_author_id',
            'author.name': 'referenced_tweet_author_name',
            'author.username': 'referenced_tweet_author_username',
            'author.description': 'referenced_tweet_author_description',
            'author.url': 'referenced_tweet_author_url',
            'author.public_metrics.followers_count': 'referenced_tweet_author_public_metrics_followers_count',
            'author.public_metrics.following_count': 'referenced_tweet_author_public_metrics_following_count',
            'author.public_metrics.tweet_count': 'referenced_tweet_author_public_metrics_tweet_count',
            'author.public_metrics.listed_count': 'referenced_tweet_author_public_metrics_listed_count',
            'author.created_at': 'referenced_tweet_author_created_at',
            'author.location': 'referenced_tweet_author_location',
            'author.pinned_tweet_id': 'referenced_tweet_author_pinned_tweet_id',
            'author.profile_image_url': 'referenced_tweet_author_profile_image_url',
            'author.protected': 'referenced_tweet_author_protected',
            'author.verified': 'referenced_tweet_author_verified'},
            errors='ignore')\
            .drop_duplicates(subset=['referenced_tweet_id', 'tweet_id', 'tweet_type'])

        # Combine tweet data with relevant referenced tweet data
        combined_level = pd.merge(reference_levels_list[i - 1], df_to_move, on='tweet_id', how='left')
        old_cols = combined_level.columns
        new_cols = old_cols.str.replace(".", "_", regex=True).str.replace("__", "", regex=True).tolist()
        cols = dict(zip(old_cols, new_cols))
        combined_level.rename(columns=cols, inplace=True)
        combined_level.sort_index(axis=1, inplace=True)
        combined_level.reset_index(drop=True, inplace=True)
        combined_levels.append(combined_level)



    if len(combined_levels) > 1:
    # Concat everything in combined_levels
        concatted = pd.concat(combined_levels)
        # Put last item in reference_levels_list at bottom
        TWEETS = pd.concat([concatted, reference_levels_list[-1]])

        # Now that 'type' has been moved up a level, rename values in 'tweet_type' column
        if 'tweet_type' in TWEETS.columns:
            TWEETS['tweet_type'] = TWEETS['tweet_type'].replace(
                {'retweeted': 'retweet',
                 'replied_to': 'reply',
                 'quoted': 'quote'})
    else:

        TWEETS = combined_level

        if 'tweet_type' in TWEETS.columns:
            TWEETS['tweet_type'] = TWEETS['tweet_type'].replace(
                {'retweeted': 'retweet',
                 'replied_to': 'reply',
                 'quoted': 'quote'})
        # if no tweet type column, that means there are no referenced tweets, therefore tweet type = original
        else:
            TWEETS['tweet_type'] = 'original'

    # if tweet_type == 'retweet', then text = referenced_tweet_text
    if 'referenced_tweet_author_username' and 'referenced_tweet_text' in TWEETS.columns:
        TWEETS['text'] = np.where(TWEETS['tweet_type'] == 'retweet', 'RT @' + TWEETS['referenced_tweet_author_username'] + ': ' + TWEETS['referenced_tweet_text'], TWEETS['text'])

    # if tweet_type == 'retweet', then entities_hashtags/mentions/urls/annotations = referenced_tweet_entities_hashtags/mentions/urls/annotations
    if 'referenced_tweet_hashtags' in TWEETS.columns:
        if 'entities_hashtags' in TWEETS.columns:
            TWEETS['entities_hashtags'] = np.where(TWEETS['tweet_type'] == 'retweet', TWEETS['referenced_tweet_hashtags'], TWEETS['entities_hashtags'])
        else:
            TWEETS['entities_hashtags'] = np.where(TWEETS['tweet_type'] == 'retweet', TWEETS['referenced_tweet_hashtags'], np.nan)

    if 'referenced_tweet_mentions' in TWEETS.columns:
        if 'entities_mentions' in TWEETS.columns:
            TWEETS['entities_mentions'] = np.where(TWEETS['tweet_type'] == 'retweet', TWEETS['referenced_tweet_mentions'], TWEETS['entities_mentions'])
        else:
            TWEETS['entities_mentions'] = np.where(TWEETS['tweet_type'] == 'retweet', TWEETS['referenced_tweet_mentions'], np.nan)

    if 'referenced_tweet_urls' in TWEETS.columns:
        if 'entities_urls' in TWEETS.columns:
            TWEETS['entities_urls'] = np.where(TWEETS['tweet_type'] == 'retweet', TWEETS['referenced_tweet_urls'], TWEETS['entities_urls'])
        else:
            TWEETS['entities_urls'] = np.where(TWEETS['tweet_type'] == 'retweet', TWEETS['referenced_tweet_urls'], np.nan)

    if 'referenced_tweet_annotations' in TWEETS.columns:
        if 'entities_annotations' in TWEETS.columns:
            TWEETS['entities_annotations'] = np.where(TWEETS['tweet_type'] == 'retweet', TWEETS['referenced_tweet_annotations'], TWEETS['entities_annotations'])
        else:
            TWEETS['entities_annotations'] = np.where(TWEETS['tweet_type'] == 'retweet', TWEETS['referenced_tweet_annotations'], np.nan)

    return TWEETS

def build_author_description_table(TWEETS, entities_mentions):
    if 'author_entities_description_hashtags' in TWEETS.columns:
        author_descr_htags = TWEETS[['author_id', 'author_entities_description_hashtags']] \
            .dropna() \
            .set_index(['author_id'])['author_entities_description_hashtags'] \
            .apply(pd.Series) \
            .stack() \
            .reset_index()
        author_descr_htags_tags = pd.json_normalize(author_descr_htags[0]) \
            .add_prefix('author_description_hashtags_')
        author_descr_htags = pd.concat(
            [author_descr_htags['author_id'], author_descr_htags_tags], axis=1)
        AUTHOR_DESCRIPTION = author_descr_htags \
            .drop_duplicates()
    else:
        AUTHOR_DESCRIPTION = pd.DataFrame(index=[0],
                                          columns=['author_id',
                                                   'author_description_hashtags_start',
                                                   'author_description_hashtags_end',
                                                   'author_description_hashtags_tag'])

    if 'author_entities_description_mentions' in TWEETS.columns:
        author_descr_mentions = TWEETS[['author_id', 'author_entities_description_mentions']] \
            .dropna() \
            .set_index(['author_id'])['author_entities_description_mentions'] \
            .apply(pd.Series) \
            .stack() \
            .reset_index()
        author_descr_mentions_mntns = pd.json_normalize(author_descr_mentions[0]) \
            .add_prefix('author_description_mentions_')
        author_descr_mentions = pd.concat([author_descr_mentions['author_id'],
                                           author_descr_mentions_mntns], axis=1)

        AUTHOR_DESCRIPTION = pd.concat([AUTHOR_DESCRIPTION, author_descr_mentions]) \
            .drop_duplicates() \
            .reset_index(drop=True)

    else:
        AUTHOR_DESCRIPTION = AUTHOR_DESCRIPTION
        AUTHOR_DESCRIPTION['author_description_mentions_start'] = np.nan
        AUTHOR_DESCRIPTION['author_description_mentions_end'] = np.nan
        AUTHOR_DESCRIPTION['author_description_mentions_username'] = np.nan

    if 'author_entities_description_urls' in TWEETS.columns:
        author_descr_urls = TWEETS[['author_id', 'author_entities_description_urls']] \
            .dropna() \
            .set_index(['author_id'])['author_entities_description_urls'] \
            .apply(pd.Series) \
            .stack() \
            .reset_index()
        author_descr_urls_urls = pd.json_normalize(author_descr_urls[0]) \
            .add_prefix('author_description_urls_')
        author_descr_urls = pd.concat([author_descr_urls['author_id'],
                                       author_descr_urls_urls], axis=1)
        AUTHOR_DESCRIPTION = pd.concat([AUTHOR_DESCRIPTION, author_descr_urls]) \
            .reset_index(drop=True) \
            .reindex(columns=DATA_fields.author_description_column_order) \
            .drop_duplicates() \
            .reset_index(drop=True)

    else:
        AUTHOR_DESCRIPTION = AUTHOR_DESCRIPTION
        AUTHOR_DESCRIPTION['author_description_urls_start'] = np.nan
        AUTHOR_DESCRIPTION['author_description_urls_end'] = np.nan
        AUTHOR_DESCRIPTION['author_description_urls_url'] = np.nan
        AUTHOR_DESCRIPTION['author_description_urls_expanded_url'] = np.nan
        AUTHOR_DESCRIPTION['author_description_urls_display_url'] = np.nan
        AUTHOR_DESCRIPTION = AUTHOR_DESCRIPTION \
            .reset_index(drop=True) \
            .reindex(columns=DATA_fields.author_description_column_order) \
            .drop_duplicates() \
            .reset_index(drop=True)

    if 'tweet_mentions_author_entities_description_hashtags' in entities_mentions.columns:
        mentioned_author_descr_htag = entities_mentions[
            ['tweet_mentions_author_id', 'tweet_mentions_author_entities_description_hashtags']] \
            .dropna() \
            .set_index(['tweet_mentions_author_id'])['tweet_mentions_author_entities_description_hashtags'] \
            .apply(pd.Series) \
            .stack() \
            .reset_index()
        mentioned_author_descr_htag_tag = pd.json_normalize(mentioned_author_descr_htag[0]) \
            .add_prefix('mentioned_author_description_hashtags_')
        MENTIONED_AUTHOR_DESCRIPTION = pd.concat(
            [mentioned_author_descr_htag['tweet_mentions_author_id'],
             mentioned_author_descr_htag_tag], axis=1)
    else:
        MENTIONED_AUTHOR_DESCRIPTION = pd.DataFrame(index=[0],
                                                    columns=['tweet_mentions_author_id',
                                                             'mentioned_author_description_hashtags_start',
                                                             'mentioned_author_description_hashtags_end',
                                                             'mentioned_author_description_hashtags_tag'])

    if 'tweet_mentions_author_entities_description_mentions' in entities_mentions.columns:
        mentioned_author_descr_mntns = entities_mentions[
            ['tweet_mentions_author_id', 'tweet_mentions_author_entities_description_mentions']] \
            .dropna() \
            .set_index(['tweet_mentions_author_id'])['tweet_mentions_author_entities_description_mentions'] \
            .apply(pd.Series) \
            .stack() \
            .reset_index()
        mentioned_author_descr_mntns_tag = pd.json_normalize(mentioned_author_descr_mntns[0]) \
            .add_prefix('mentioned_author_description_mentions_')
        mentioned_author_descr_mntns = pd.concat(
            [mentioned_author_descr_mntns['tweet_mentions_author_id'],
             mentioned_author_descr_mntns_tag], axis=1)
        try:
            MENTIONED_AUTHOR_DESCRIPTION = pd.concat(
                [MENTIONED_AUTHOR_DESCRIPTION, mentioned_author_descr_mntns])
        except:
            MENTIONED_AUTHOR_DESCRIPTION = MENTIONED_AUTHOR_DESCRIPTION
    else:
        MENTIONED_AUTHOR_DESCRIPTION = MENTIONED_AUTHOR_DESCRIPTION
        MENTIONED_AUTHOR_DESCRIPTION['mentioned_author_description_mentions_start'] = np.nan
        MENTIONED_AUTHOR_DESCRIPTION['mentioned_author_description_mentions_end'] = np.nan
        MENTIONED_AUTHOR_DESCRIPTION['mentioned_author_description_mentions_username'] = np.nan

    if 'tweet_mentions_author_entities_description_urls' in entities_mentions.columns:
        mentioned_author_descr_urls = entities_mentions[
            ['tweet_mentions_author_id', 'tweet_mentions_author_entities_description_urls']] \
            .dropna() \
            .set_index(['tweet_mentions_author_id'])['tweet_mentions_author_entities_description_urls'] \
            .apply(pd.Series) \
            .stack() \
            .reset_index()
        mentioned_author_descr_urls_urls = pd.json_normalize(mentioned_author_descr_urls[0]) \
            .add_prefix('mentioned_author_description_urls_')
        mentioned_author_descr_urls = pd.concat(
            [mentioned_author_descr_urls['tweet_mentions_author_id'],
             mentioned_author_descr_urls_urls], axis=1)
        try:
            MENTIONED_AUTHOR_DESCRIPTION = pd.concat(
                [MENTIONED_AUTHOR_DESCRIPTION, mentioned_author_descr_urls])
        except:
            MENTIONED_AUTHOR_DESCRIPTION = MENTIONED_AUTHOR_DESCRIPTION \
                .reset_index(drop=True) \
                .reindex(columns=DATA_fields.author_description_mentions_column_order) \
                .drop_duplicates()
    else:
        MENTIONED_AUTHOR_DESCRIPTION = MENTIONED_AUTHOR_DESCRIPTION
        MENTIONED_AUTHOR_DESCRIPTION['mentioned_author_description_urls_start'] = np.nan
        MENTIONED_AUTHOR_DESCRIPTION['mentioned_author_description_urls_end'] = np.nan
        MENTIONED_AUTHOR_DESCRIPTION['mentioned_author_description_urls_url'] = np.nan
        MENTIONED_AUTHOR_DESCRIPTION['mentioned_author_description_urls_expanded_url'] = np.nan
        MENTIONED_AUTHOR_DESCRIPTION['mentioned_author_description_urls_display_url'] = np.nan

    if 'in_reply_to_user_entities_description_hashtags' in TWEETS.columns:
        in_reply_to_user_descr_htags = TWEETS[['in_reply_to_user_id', 'in_reply_to_user_entities_description_hashtags']] \
            .dropna() \
            .set_index(['in_reply_to_user_id'])['in_reply_to_user_entities_description_hashtags'] \
            .apply(pd.Series) \
            .stack() \
            .reset_index()
        in_reply_to_user_descr_htags_tags = pd.json_normalize(in_reply_to_user_descr_htags[0]) \
            .add_prefix('in_reply_to_user_description_hashtags_')
        in_reply_to_user_descr_htags = pd.concat([in_reply_to_user_descr_htags['in_reply_to_user_id'],
                                                  in_reply_to_user_descr_htags_tags], axis=1)
        IN_REPLY_TO_USER_DESCRIPTION = in_reply_to_user_descr_htags
    else:
        IN_REPLY_TO_USER_DESCRIPTION = pd.DataFrame(index=[0],
                                                    columns=['in_reply_to_user_id',
                                                             'in_reply_to_user_description_hashtags_start',
                                                             'in_reply_to_user_description_hashtags_end',
                                                             'in_reply_to_user_description_hashtags_tag'])

    if 'in_reply_to_user_entities_description_mentions' in TWEETS.columns:
        in_reply_to_user_descr_mntns = TWEETS[['in_reply_to_user_id', 'in_reply_to_user_entities_description_mentions']] \
            .dropna() \
            .set_index(['in_reply_to_user_id'])['in_reply_to_user_entities_description_mentions'] \
            .apply(pd.Series) \
            .stack() \
            .reset_index()
        in_reply_to_user_descr_mntns_mntns = pd.json_normalize(in_reply_to_user_descr_mntns[0]) \
            .add_prefix('in_reply_to_user_description_mentions_')
        in_reply_to_user_descr_mntns = pd.concat([in_reply_to_user_descr_mntns['in_reply_to_user_id'],
                                                  in_reply_to_user_descr_mntns_mntns], axis=1)

        IN_REPLY_TO_USER_DESCRIPTION = pd.concat([IN_REPLY_TO_USER_DESCRIPTION, in_reply_to_user_descr_mntns])
    else:
        IN_REPLY_TO_USER_DESCRIPTION = IN_REPLY_TO_USER_DESCRIPTION
        IN_REPLY_TO_USER_DESCRIPTION['in_reply_to_user_description_mentions_start'] = np.nan
        IN_REPLY_TO_USER_DESCRIPTION['in_reply_to_user_description_mentions_end'] = np.nan
        IN_REPLY_TO_USER_DESCRIPTION['in_reply_to_user_description_mentions_username'] = np.nan

    if 'in_reply_to_user_entities_description_urls' in TWEETS.columns:
        in_reply_to_user_descr_urls = \
            TWEETS[['in_reply_to_user_id', 'in_reply_to_user_entities_description_urls']] \
                .dropna() \
                .set_index(['in_reply_to_user_id'])['in_reply_to_user_entities_description_urls'] \
                .apply(pd.Series) \
                .stack() \
                .reset_index()
        in_reply_to_user_descr_urls_urls = pd.json_normalize(in_reply_to_user_descr_urls[0]) \
            .add_prefix('in_reply_to_user_description_urls_')
        in_reply_to_user_descr_urls = pd.concat(
            [in_reply_to_user_descr_urls['in_reply_to_user_id'], in_reply_to_user_descr_urls_urls], axis=1)
        IN_REPLY_TO_USER_DESCRIPTION = pd.concat([IN_REPLY_TO_USER_DESCRIPTION, in_reply_to_user_descr_urls])
    else:
        IN_REPLY_TO_USER_DESCRIPTION = IN_REPLY_TO_USER_DESCRIPTION
        IN_REPLY_TO_USER_DESCRIPTION['in_reply_to_user_description_urls_start'] = np.nan
        IN_REPLY_TO_USER_DESCRIPTION['in_reply_to_user_description_urls_end'] = np.nan
        IN_REPLY_TO_USER_DESCRIPTION['in_reply_to_user_description_urls_url'] = np.nan
        IN_REPLY_TO_USER_DESCRIPTION['in_reply_to_user_description_urls_expanded_url'] = np.nan
        IN_REPLY_TO_USER_DESCRIPTION['in_reply_to_user_description_urls_display_url'] = np.nan

    author_description_colnames = AUTHOR_DESCRIPTION.columns
    IN_REPLY_TO_USER_DESCRIPTION.columns = author_description_colnames
    MENTIONED_AUTHOR_DESCRIPTION.columns = author_description_colnames
    AUTHOR_DESCRIPTION = pd.concat([AUTHOR_DESCRIPTION, IN_REPLY_TO_USER_DESCRIPTION, MENTIONED_AUTHOR_DESCRIPTION])

    logging.info('AUTHOR_DESCRIPTION table built')
    return AUTHOR_DESCRIPTION

def build_author_urls_table(TWEETS, entities_mentions):
    if 'author_entities_url_urls' in TWEETS.columns:
        author_urls = TWEETS[['author_id', 'author_entities_url_urls']].dropna() \
            .set_index(['author_id'])['author_entities_url_urls'] \
            .apply(pd.Series) \
            .stack() \
            .reset_index()
        author_urls_urls = pd.json_normalize(author_urls[0]) \
            .add_prefix('author_')
        AUTHOR_URLS = pd.concat(
            [author_urls['author_id'], author_urls_urls], axis=1) \
            .reset_index(drop=True) \
            .reindex(columns=DATA_fields.author_urls_column_order) \
            .drop_duplicates() \
            .reset_index(drop=True)
    else:
        AUTHOR_URLS = pd.DataFrame(index=[0],
                                   columns=['author_id',
                                            'author_urls_start',
                                            'author_urls_end',
                                            'author_urls_url',
                                            'author_urls_expanded_url',
                                            'author_urls_display_url'])
    if 'tweet_mentions_author_entities_url_urls' in entities_mentions.columns:
        mentioned_author_urls = entities_mentions[
            ['tweet_mentions_author_id', 'tweet_mentions_author_entities_url_urls']] \
            .dropna() \
            .set_index(['tweet_mentions_author_id'])['tweet_mentions_author_entities_url_urls'] \
            .apply(pd.Series) \
            .stack() \
            .reset_index()
        mentioned_author_urls_urls = pd.json_normalize(mentioned_author_urls[0]) \
            .add_prefix('mentioned_author_')
        MENTIONED_AUTHOR_URLS = pd.concat(
            [mentioned_author_urls['tweet_mentions_author_id'],
             mentioned_author_urls_urls], axis=1)
    else:
        MENTIONED_AUTHOR_URLS = pd.DataFrame(index=[0],
                                             columns=['mentioned_author_id',
                                                      'mentioned_author_urls_start',
                                                      'mentioned_author_urls_end',
                                                      'mentioned_author_urls_url',
                                                      'mentioned_author_urls_expanded_url',
                                                      'mentioned_author_urls_display_url'])
    if 'in_reply_to_user_entities_url_urls' in TWEETS.columns:
        in_reply_to_user_urls = TWEETS[['in_reply_to_user_id', 'in_reply_to_user_entities_url_urls']] \
            .dropna() \
            .set_index(['in_reply_to_user_id'])['in_reply_to_user_entities_url_urls'] \
            .apply(pd.Series) \
            .stack() \
            .reset_index()
        in_reply_to_user_urls_urls = pd.json_normalize(in_reply_to_user_urls[0]) \
            .add_prefix('in_reply_to_user_')
        in_reply_to_user_urls = pd.concat([in_reply_to_user_urls['in_reply_to_user_id'],
                                           in_reply_to_user_urls_urls], axis=1) \
            .reset_index(drop=True)
        IN_REPLY_TO_USER_URLS = in_reply_to_user_urls
    else:
        IN_REPLY_TO_USER_URLS = pd.DataFrame(index=[0],
                                             columns=['in_reply_to_user_id',
                                                      'in_reply_to_user_urls_start',
                                                      'in_reply_to_user_urls_end',
                                                      'in_reply_to_user_urls_url',
                                                      'in_reply_to_user_urls_expanded_url',
                                                      'in_reply_to_user_urls_display_url'])
    author_urls_colnames = AUTHOR_URLS.columns
    IN_REPLY_TO_USER_URLS.columns = author_urls_colnames
    MENTIONED_AUTHOR_URLS.columns = author_urls_colnames
    AUTHOR_URLS = pd.concat([AUTHOR_URLS, IN_REPLY_TO_USER_URLS, MENTIONED_AUTHOR_URLS])
    logging.info('AUTHOR_URLS table built')
    return AUTHOR_URLS

def build_media_table(TWEETS):
    if 'media' in TWEETS.columns:
        TWEETS['attachments_media'] = TWEETS['media']
    if 'attachments_media' in TWEETS.columns:
        media_data = TWEETS[['tweet_id', 'attachments_media']] \
            .dropna() \
            .set_index(['tweet_id'])['attachments_media'] \
            .apply(pd.Series) \
            .stack() \
            .reset_index()
        media_data_media = pd.json_normalize(media_data[0]) \
            .add_prefix('media_')
        media_data = pd.concat([media_data['tweet_id'], media_data_media], axis=1) \
            .rename(columns={
            'media_public_metrics.view_count': 'media_public_metrics_view_count',
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

def build_poll_options_table(TWEETS):
    if 'attachments_poll_options' in TWEETS.columns:
        poll_options = TWEETS[['attachments_poll_id', 'attachments_poll_options']] \
            .dropna() \
            .set_index(['attachments_poll_id'])['attachments_poll_options'] \
            .apply(pd.Series) \
            .stack() \
            .reset_index()
        poll_options_polls = pd.json_normalize(poll_options[0]) \
            .add_prefix('poll_')
        POLL_OPTIONS = pd.concat([poll_options['attachments_poll_id'], poll_options_polls], axis=1) \
            .astype({'poll_position': 'object'}) \
            .rename(columns={'attachments_poll_id': 'poll_id'}) \
            .reset_index(drop=True) \
            .reindex(columns=DATA_fields.poll_options_column_order) \
            .drop_duplicates() \
            .reset_index(drop=True)
    else:
        POLL_OPTIONS = None
    logging.info('POLL_OPTIONS table built')
    return POLL_OPTIONS

def build_context_annotations_table(TWEETS):
    if 'context_annotations' in TWEETS.columns:
        context_annotations = TWEETS[['tweet_id', 'context_annotations']] \
            .dropna() \
            .set_index(['tweet_id'])['context_annotations'] \
            .apply(pd.Series) \
            .stack() \
            .reset_index()
        context_annotations_ann = pd.json_normalize(context_annotations[0]) \
            .add_prefix('tweet_context_annotation_')
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

def build_annotations_table(TWEETS):
    if 'entities_annotations' in TWEETS.columns:
        entities_annotations = TWEETS[['tweet_id', 'entities_annotations']] \
            .dropna() \
            .set_index(['tweet_id'])['entities_annotations'] \
            .apply(pd.Series) \
            .stack() \
            .reset_index()
        entities_annotations_ann = pd.json_normalize(entities_annotations[0]) \
            .add_prefix('tweet_annotation_')
        ANNOTATIONS = pd.concat([entities_annotations['tweet_id'], entities_annotations_ann], axis=1) \
            .reset_index(drop=True) \
            .reindex(columns=DATA_fields.annotations_column_order) \
            .drop_duplicates() \
            .reset_index(drop=True)
    else:
        ANNOTATIONS = None
    logging.info('ANNOTATIONS table built')
    return ANNOTATIONS

def build_hashtags_table(TWEETS):
    if 'entities_hashtags' in TWEETS.columns:
        entities_hashtags = TWEETS[['tweet_id', 'entities_hashtags']] \
            .dropna() \
            .set_index(['tweet_id'])['entities_hashtags'] \
            .apply(pd.Series) \
            .stack() \
            .reset_index()
        entities_hashtags_tag = pd.json_normalize(entities_hashtags[0]) \
            .add_prefix('hashtags_')
        HASHTAGS = pd.concat([entities_hashtags['tweet_id'], entities_hashtags_tag], axis=1) \
            .dropna(subset='hashtags_tag') \
            .reset_index(drop=True) \
            .reindex(columns=DATA_fields.hashtags_column_order) \
            .drop_duplicates() \
            .reset_index(drop=True)
    else:
        HASHTAGS = None
    logging.info('HASHTAGS table built')
    return HASHTAGS

def extract_entities_data(TWEETS):
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
    entities_mentions.columns = entities_mentions.columns.str.replace(".", "_", regex=True)
    # Replace nans in int fields with 0
    for column in entities_mentions.columns:
        if entities_mentions[column].dtype == float:
            entities_mentions[column] = entities_mentions[column].fillna(value=0)
            entities_mentions[column] = entities_mentions[column].astype('int32')

    return entities_mentions

def build_mentions_table(entities_mentions):
        MENTIONS = entities_mentions \
            .reset_index(drop=True) \
            .reindex(columns=DATA_fields.mention_column_order) \
            .drop_duplicates()
        logging.info('MENTIONS table built')
        return MENTIONS

def build_urls_table(TWEETS):
    if 'entities_urls' in TWEETS.columns:
        entities_urls = TWEETS[['tweet_id', 'entities_urls']] \
            .dropna() \
            .set_index(['tweet_id'])['entities_urls'] \
            .apply(pd.Series) \
            .stack() \
            .reset_index()
        entities_urls_urls = pd.json_normalize(entities_urls[0]) \
            .add_prefix('urls_')
        # TODO: DEAL WITH URLS_IMAGES
        if 'urls_images' in entities_urls_urls.columns:
            entities_urls_urls = entities_urls_urls.drop(columns=['urls_images'])
        URLS = pd.concat([entities_urls['tweet_id'], entities_urls_urls], axis=1) \
            .reset_index(drop=True) \
            .reindex(columns=DATA_fields.urls_column_order) \
            .drop_duplicates()
    else:
        URLS = None
    logging.info('URLS table built')

    return URLS

def build_interactions_table(TWEETS, URLS, MENTIONS):
    # if tweet_type = quote AND referenced_tweet_author_username = nan,
    # extract quoted tweet usernames from embedded urls and replace nan with extracted username
    if URLS is not None:
        if 'referenced_tweet_author_username' in TWEETS.columns:
            TWEETS = TWEETS.merge(URLS[['tweet_id', 'urls_expanded_url']], how='left', on='tweet_id')
            for i in range(len(TWEETS)):
                try:
                    TWEETS['urls_expanded_url'][i] = TWEETS['urls_expanded_url'][i].split('/')[3]
                    TWEETS['urls_expanded_url'][i] = TWEETS['urls_expanded_url'][i].replace('@', '')
                except:
                    continue
            TWEETS['referenced_tweet_author_username'] = np.where(TWEETS['tweet_type'] == 'quote',
                                                                  TWEETS['referenced_tweet_author_username'] \
                                                                  .fillna(TWEETS['urls_expanded_url']),
                                                                  TWEETS['referenced_tweet_author_username'])

    # If tweet_type = reply AND referenced_tweet_author_username = nan,
    # extract reply tweet usernames from tweet_text and replace nan with extracted username
    if 'referenced_tweet_author_username' in TWEETS.columns:
        TWEETS['reply_usernames'] = ''
        for i in range(len(TWEETS)):
            try:
                TWEETS['reply_usernames'][i] = TWEETS['text'][i].split(' ')[0]
                TWEETS['reply_usernames'][i] = TWEETS['reply_usernames'][i].replace('@', '')
            except:
                continue
        TWEETS['referenced_tweet_author_username'] = np.where(TWEETS['tweet_type'] == 'reply',
                                                              TWEETS['referenced_tweet_author_username'] \
                                                              .fillna(TWEETS['reply_usernames']),
                                                              TWEETS['referenced_tweet_author_username'])

    if 'referenced_tweet_author_id' in TWEETS.columns:
        interactions = TWEETS[[
            'tweet_id',
            'tweet_type',
            'referenced_tweet_author_id',
            'referenced_tweet_author_username']] \
            .rename(columns={
            'referenced_tweet_author_id': 'to_user_id',
            'referenced_tweet_author_username': 'to_user_username'})

        if MENTIONS is not None:
            interactions_mentions = MENTIONS[[
                'tweet_id',
                'tweet_mentions_author_id',
                'tweet_mentions_author_username']] \
                .rename(columns={
                'tweet_mentions_author_id': 'to_user_id',
                'tweet_mentions_author_username': 'to_user_username'})
            interactions_mentions['tweet_type'] = 'mention'
            interactions = interactions \
                .drop(interactions.index[interactions['tweet_type'] == 'original'])
            INTERACTIONS = pd.concat([interactions, interactions_mentions], ignore_index=True)
            INTERACTIONS = INTERACTIONS \
                .reset_index(drop=True) \
                .dropna(subset='tweet_type') \
                .reindex(columns=DATA_fields.interactions_column_order) \
                .drop_duplicates()
        else:
            INTERACTIONS = None

    else:
        INTERACTIONS = None
    logging.info('INTERACTIONS table built')

    return INTERACTIONS

def get_schema_type(list_of_dataframes, tweet_count):
    if Schematype.DATA == True:
        list_of_dataframes = list_of_dataframes
        list_of_csv = DATA_schema.list_of_csv
        list_of_tablenames = DATA_schema.list_of_tablenames
        list_of_schema = DATA_schema.list_of_schema
        tweet_count = len(list_of_dataframes[0].loc[list_of_dataframes[0]['reference_level'] == '0']) + tweet_count
    elif Schematype.TCAT == True:
        list_of_dataframes = transform_DATA_to_TCAT(list_of_dataframes)
        list_of_csv = TCAT_schema.list_of_csv
        list_of_tablenames = TCAT_schema.list_of_tablenames
        list_of_schema = TCAT_schema.list_of_schema
        tweet_count = len(list_of_dataframes[0]) + tweet_count
    else:
        list_of_dataframes = []
        list_of_csv = TweetQuery_schema.list_of_csv
        list_of_tablenames = TweetQuery_schema.list_of_tablenames
        list_of_schema = TweetQuery_schema.list_of_schema
        tweet_count = len(list_of_dataframes[0]) + tweet_count

    return list_of_dataframes, list_of_csv, list_of_tablenames, list_of_schema, tweet_count

def transform_DATA_to_TCAT(list_of_dataframes):
    # ------------------------------------------------------------------------------------------------------
    # RENAME TWARC FIELDS FOR TCAT COMPATIBILITY
    # ------------------------------------------------------------------------------------------------------
    TWEETS = list_of_dataframes[0].copy()
    HASHTAGS = list_of_dataframes[4].copy()
    MENTIONS = list_of_dataframes[6].copy()


    # HASHTAGS
    # --------
    HASHTAGS = HASHTAGS[TCAT_fields.hashtags_column_order]\
        .rename(columns=TCAT_fields.hashtags_column_names_dict)

    # MENTIONS
    # --------
    interactions = TWEETS[[
        'tweet_id',
        'tweet_type',
        'referenced_tweet_author_id',
        'referenced_tweet_author_username'
    ]].astype('string')

    interactions_author = TWEETS[['tweet_id', 'author_id', 'author_username']]
    interactions = interactions.merge(interactions_author, how='left', on='tweet_id')

    try:
        interactions = interactions.drop(interactions.index[interactions['tweet_type'] == 'original'])
    except:
        pass

    interactions = interactions\
        .dropna()\
        .rename(columns=TCAT_fields.interactions_mentions_column_names_dict)

    if MENTIONS is not None:
        interactions_mentions = MENTIONS[[
            'tweet_id',
            'tweet_mentions_author_id',
            'tweet_mentions_author_username'
        ]].astype('string')
        interactions_mentions = interactions_mentions.merge(interactions_author, how='left', on='tweet_id')

        interactions_mentions['tweet_type'] = 'mention'

        MENTIONS = pd.concat([interactions, interactions_mentions], ignore_index=True)

        MENTIONS = MENTIONS[TCAT_fields.mentions_column_order] \
            .rename(columns=TCAT_fields.mentions_column_names_dict) \
            .drop_duplicates()
    else:
        MENTIONS = interactions.drop_duplicates()



    # TWEETS
    # ------
    TWEETS['time'] = TWEETS['created_at']
    TWEETS['in_reply_to_status_id'] = TWEETS['referenced_tweet_id']

    TWEETS['quoted_status_id'] = TWEETS['referenced_tweet_id']
    TWEETS['author_created_at'] = pd.to_datetime(TWEETS["author_created_at"], format="%Y-%m-%dT%H:%M:%S")

    TWEETS.loc[TWEETS['tweet_type'] != 'quote', 'quoted_status_id'] = ''
    TWEETS.loc[TWEETS['tweet_type'] != 'reply', 'in_reply_to_status_id'] = ''

    TWEETS['filter_level'] = ''
    TWEETS['withheld_copyright'] = ''
    TWEETS['withheld_scope'] = ''
    TWEETS['truncated'] = ''
    TWEETS['lat'] = ''
    TWEETS['lng'] = ''
    TWEETS['from_user_utcoffset'] = ''
    TWEETS['from_user_timezone'] = ''
    TWEETS['from_user_lang'] = ''
    TWEETS['from_user_favourites_count'] = ''
    TWEETS['from_user_withheld_scope'] = ''

    TWEETS = TWEETS[TCAT_fields.tweet_column_order] \
        .rename(columns=TCAT_fields.tweet_column_names_dict)

    list_of_dataframes = [TWEETS, HASHTAGS, MENTIONS]

    return list_of_dataframes

def write_processed_data_to_csv(tweetframe, csv_file, csv_filepath):
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

def push_processed_tables_to_bq(bq, project, dataset, list_of_tablenames, csv_filepath, list_of_csv, query, start_date, end_date, list_of_schema, list_of_dataframes):

    logging.info('Pushing tables to Google BigQuery database.')

    tweet_dataset = f"{project}.{dataset}"
    num_tables = len(list_of_tablenames)
    # TODO check this description stuff
    # Create dataset if one does not exist
    try:
        bq.get_dataset(tweet_dataset)

    except NotFound:
        logging.info(f"Dataset {tweet_dataset} is not found.")
        bq.create_dataset(tweet_dataset)

        ds = bq.get_dataset(tweet_dataset)
        ds.description = f"Query: {query}, {start_date}, {end_date}"
        ds = bq.update_dataset(ds, ["description"])
        logging.info(f"Created new dataset: {tweet_dataset}.")

    # Create table if one does not exist
    for i in range(num_tables):
        if os.path.isfile(csv_filepath + list_of_csv[i]) == True:
            table_id = bigquery.Table(tweet_dataset + '.' + list_of_tablenames[i])
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
                job = bq.load_table_from_file(tweet_fh, tweet_dataset + '.' + list_of_tablenames[i],
                                              job_config=job_config)
                job.result()

            table = bq.get_table(table_id)
            logging.info(f"Loaded {len(list_of_dataframes[i])} rows and {len(table.schema)} columns "
                         f"to {table.project}.{table.dataset_id}.{table.table_id}")

    fileList = glob.glob(csv_filepath + '*csv')
    # Iterate over the list of filepaths & remove each file.
    for filePath in fileList:
        os.remove(filePath)

    return table

    logging.info('-----------------------------------------------------------------------------------------')

def query_total_record_count(table, bq):
    # Query total rows in dataset
    if Schematype.DATA == True:
        # Query number of level 0 tweets in dataset
        counts_query_string = f"""
            SELECT
            *
            FROM `{table.project}.{table.dataset_id}.tweets`
            WHERE reference_level = '0'
            """
        total_rows_tweet_count = (bq.query(counts_query_string).result()).total_rows
    else:
        counts_query_string = f"""
            SELECT
            *
            FROM `{table.project}.{table.dataset_id}.tweets`
            """
        total_rows_tweet_count = (bq.query(counts_query_string).result()).total_rows

    return total_rows_tweet_count

def capture_error_string(error, error_filepath):
    error_string = repr(error)
    logging.info(error_string)
    traceback_info = traceback.format_exc()
    logging.info(traceback_info)
    error_time = re.sub("[^0-9]", "", str(datetime.now().replace(microsecond=0)))
    text_file = open(f'{error_filepath}error_log_{error_time}.txt', "w")
    n = text_file.write(traceback_info)
    text_file.close()
    logging.info('Error log written to file.')
    return traceback_info

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
    time.sleep(5)
    # Search parameters
    # -----------------
    query = Query.query
    bearer_token = Tokens.bearer_token
    start_date = Query.start_date
    end_date = Query.end_date
    project = GBQ.project_id
    dataset = GBQ.dataset
    interval = Query.interval_days

    # Validate search parameters
    query, bearer_token, access_key, start_date, end_date, project, dataset, interval = validate_search_parameters(query, bearer_token, start_date, end_date, project, dataset, interval)

    if Schematype.DATA == True:
        schematype = 'DATA'
    elif Schematype.TCAT == True:
        schematype = 'TCAT'
    else:
        schematype = 'TweetQuery'

    # Initiate a Twarc client instance
    client = Twarc2(bearer_token=bearer_token)

    # Pre-search archive counts
    archive_search_counts, user_proceed = get_pre_search_counts(client, query, start_date, end_date, project, dataset, schematype, interval)

    if user_proceed == 'y':
        sleep(3)
        # Set directories and file paths
        # ------------------------------
        set_up_logging(logfile_filepath)
        sleep(1)
        set_directory(dir_name, folder)
        sleep(1)
        set_json_path(json_filepath, folder)
        sleep(1)
        set_csv_path(csv_filepath, folder)
        sleep(1)
        set_error_log_path(error_filepath, folder)
        sleep(1)
        set_log_file_path(logfile_filepath, folder)
        sleep(1)

        # Access BigQuery
        # ---------------
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = access_key[0]
        bq = Client(project=project)




        # Set table variable to none, if it gets a value it can be queried
        table = 0
        tweet_count = 0

        # TODO: why two try/excepts here?
        try:
            try:
                # Get current datetime for calculating duration
                search_start_time = datetime.now()

                # to_collect, expected files tell the program what to collect and what has already been collected
                to_collect, expected_files = set_up_expected_files(start_date, end_date, json_filepath)
                # Call function collect_archive_data()
                collect_archive_data(bq, project, dataset, to_collect, expected_files, client, query, start_date, end_date, csv_filepath, archive_search_counts, tweet_count)
                table = 1
                search_end_time = datetime.now()
                search_duration = (search_end_time - search_start_time)
                readable_duration = humanfriendly.format_timespan(search_duration)

                if table > 0:
                    table_id = bigquery.Table(f'{project}.{dataset}.tweets')
                    table = bq.get_table(table_id)
                    total_rows_tweet_count = query_total_record_count(table, bq)
                    time.sleep(30)
                    send_completion_email(mailgun_domain, mailgun_key, query, start_date, end_date, total_rows_tweet_count,
                                          search_start_time, search_end_time, readable_duration, num_rows=table.num_rows,
                                          project=table.project, dataset=table.dataset_id)
                    logging.info('Completion email sent to user.')
                else:
                    time.sleep(30)
                    send_no_results_email(mailgun_domain, mailgun_key, query, start_date, end_date)
                    logging.info('No results email sent to user.')

                logging.info('Archive search complete!')

            except Exception as error:

                traceback_info = capture_error_string(error, error_filepath)
                send_error_email(mailgun_domain, mailgun_key, dataset, traceback_info)
                logging.info('Error email sent to admin.')

        except Exception as error:
            traceback_info = capture_error_string(error, error_filepath)
            send_error_email(mailgun_domain, mailgun_key, dataset, traceback_info)
            logging.info('Error email sent to admin.')

    else:
        exit()
