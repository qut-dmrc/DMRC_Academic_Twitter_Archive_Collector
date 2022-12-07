'''
Contains all functions relating to the separate processing of the nested columns in Twarc json data. Each nested colum
becomes its own flattened table and is linked to the main TWEETS table on tweet_id, author_id or poll_id
'''

import pandas as pd
import numpy as np

from .fields import DATA_fields, TCAT_fields, TweetQuery_fields, TWEET_fields
from .set_up_directories import *

pd.options.mode.chained_assignment = None

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)


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
                # # These boolean columns produce true if exists and false if not; problematic for lower ref levels where data are missing false negatives, so if ref_level != 0 keep nans
                # TWEETS[bool_col] = TWEETS[col].isnull().map({True: 'false', False: 'true'})
                TWEETS[bool_col] = TWEETS[col].loc[TWEETS['reference_level'] == '0'].isnull().map({True: 'false', False: 'true'})
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

        # # Convert boolean columns
        # TWEETt = TWEETS
        # TWEETS[boolean_cols] = TWEETS[boolean_cols].astype('boolean')

        return TWEETS

    def fill_blanks_and_nas(self, TWEETS):
        '''
        Converts converts blanks to nans; converts nans in int and float fields to 0 for consistency and to prevent ValueError.
        '''

        # Convert any blanks to nan for workability
        # TWEETSy = TWEETS.replace('', np.nan, regex=True)

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

        entities_mentions = TWEETS[['tweet_id', 'entities_mentions']].dropna()
        if len(entities_mentions) > 0:
            entities_mentions = entities_mentions \
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