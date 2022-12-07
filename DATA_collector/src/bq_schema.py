'''
This file contains the Google BigQuery table schema for each schematype as specified in config.yml
'''

from google.cloud import bigquery
from .config import Schematype
import numpy as np
import pandas as pd


class DATA_schema:

    tweet_schema = [
        bigquery.SchemaField("tweet_id", "STRING", mode="REQUIRED",
                             description="Unique ID for this Tweet (from Twitter)"),
        bigquery.SchemaField("tweet_text", "STRING", mode="NULLABLE", description="Text of the Tweet"),
        bigquery.SchemaField("tweet_type", "STRING", mode="NULLABLE", description="Type of Tweet"),
        bigquery.SchemaField("created_at", "TIMESTAMP", mode="NULLABLE",
                             description="When the Tweet was created (UTC)"),
        bigquery.SchemaField("public_metrics_like_count", "INTEGER", mode="NULLABLE",
                             description="Count of likes of Tweet"),
        bigquery.SchemaField("public_metrics_quote_count", "INTEGER", mode="NULLABLE",
                             description="Count of quotes of Tweet"),
        bigquery.SchemaField("public_metrics_reply_count", "INTEGER", mode="NULLABLE",
                             description="Count of replies to Tweet"),
        bigquery.SchemaField("public_metrics_retweet_count", "INTEGER", mode="NULLABLE",
                             description="Count of retweets of Tweet"),
        bigquery.SchemaField("is_retweet", "BOOL", mode="NULLABLE",
                             description="Whether this Tweet is a retweet of another Tweet"),
        bigquery.SchemaField("is_reply", "BOOL", mode="NULLABLE",
                             description="Whether this Tweet is a reply to another Tweet"),
        bigquery.SchemaField("is_quote", "BOOL", mode="NULLABLE",
                             description="Whether this Tweet quotes another Tweet"),
        bigquery.SchemaField("has_mention", "BOOL", mode="NULLABLE",
                             description="Whether this Tweet mentions a user or entity"),
        bigquery.SchemaField("has_media", "BOOL", mode="NULLABLE",
                             description="Whether this Tweet contains a media item (image, video or animated GIF)"),
        bigquery.SchemaField("has_hashtags", "BOOL", mode="NULLABLE",
                             description="Whether this Tweet contains hashtags"),
        bigquery.SchemaField("has_urls", "BOOL", mode="NULLABLE",
                             description="Whether this Tweet contains urls"),
        bigquery.SchemaField("has_annotations", "BOOL", mode="NULLABLE",
                             description="Whether this Tweet has accompanying annotations"),
        bigquery.SchemaField("has_context_annotations", "BOOL", mode="NULLABLE",
                             description="Whether this has accompanying context annotations"),
        bigquery.SchemaField("lang", "STRING", mode="NULLABLE",
                             description="Language of the Tweet, if detected by Twitter"),
        bigquery.SchemaField("possibly_sensitive", "BOOL", mode="NULLABLE",
                             description="Indicates whether the URL contained in the Tweet may contain content or media identified as sensitive content"),
        bigquery.SchemaField("conversation_id", "STRING", mode="NULLABLE",
                             description="The Tweet ID of the original Tweet of the conversation"),
        bigquery.SchemaField("reply_settings", "STRING", mode="NULLABLE",
                             description="Shows who can reply to a given Tweet"),
        bigquery.SchemaField("source", "STRING", mode="NULLABLE",
                             description="The name of the app the user Tweeted from."),
        bigquery.SchemaField("is_referenced", "BOOL", mode="NULLABLE",
                             description="Whether this Tweet has been referenced by another in this dataset"),
        bigquery.SchemaField("reference_level", "STRING", mode="NULLABLE",
                             description="Reference level. 0=Collected Tweets (matching search parameters); 1=Referenced Tweets; 2=Tweets referenced by Referenced Tweets; 3=Tweets referenced by Tweets referenced by Referenced Tweets"),
        bigquery.SchemaField("referencing_tweet_id", "STRING", mode="NULLABLE",
                             description="The unique ID of the Tweet that REFERENCES this Tweet"),
        bigquery.SchemaField("referenced_tweet_id", "STRING", mode="NULLABLE",
                             description="The unique ID of the Tweet REFERENCED BY this Tweet"),
        bigquery.SchemaField("author_id", "STRING", mode="NULLABLE",
                             description="The unique identifier of the user who posted this Tweet"),
        bigquery.SchemaField("author_username", "STRING", mode="NULLABLE",
                             description="The username of the user who posted this Tweet"),
        bigquery.SchemaField("author_name", "STRING", mode="NULLABLE",
                             description="The name of the user who posted this Tweet"),
        bigquery.SchemaField("author_description", "STRING", mode="NULLABLE",
                             description="The text of the Tweet author's profile description"),
        bigquery.SchemaField("author_url", "STRING", mode="NULLABLE",
                             description="The URL on the profile of the the Tweet author"),
        bigquery.SchemaField("author_public_metrics_followers_count", "INTEGER", mode="NULLABLE",
                             description="The number of entites following the Tweet author"),
        bigquery.SchemaField("author_public_metrics_following_count", "INTEGER", mode="NULLABLE",
                             description="The number of entities the Tweet author follows"),
        bigquery.SchemaField("author_public_metrics_tweet_count", "INTEGER", mode="NULLABLE",
                             description="The total number of Tweets posted by the Tweet author"),
        bigquery.SchemaField("author_public_metrics_listed_count", "INTEGER", mode="NULLABLE",
                             description="The number of public lists that the Tweet author is a member of"),
        bigquery.SchemaField("author_created_at", "TIMESTAMP", mode="NULLABLE",
                             description="When the Tweet author's account was created (UTC)"),
        bigquery.SchemaField("author_location", "STRING", mode="NULLABLE",
                             description="Location of Tweet author"),
        bigquery.SchemaField("author_pinned_tweet_id", "STRING", mode="NULLABLE",
                             description="ID of Tweet author's pinned Tweet"),
        bigquery.SchemaField("author_profile_image_url", "STRING", mode="NULLABLE",
                             description="URL of the Tweet author's profile image"),
        bigquery.SchemaField("author_protected", "BOOL", mode="NULLABLE",
                             description="Whether or not the Tweet author's Tweets are visible only to followers"),
        bigquery.SchemaField("author_verified", "BOOL", mode="NULLABLE",
                             description="Whether or not the Tweet author's account has been verified by Twitter"),
        bigquery.SchemaField("referenced_tweet_text", "STRING", mode="NULLABLE",
                             description="The text of the Tweet that has been referenced"),
        bigquery.SchemaField("referenced_tweet_author_id", "STRING", mode="NULLABLE",
                             description="The unique ID of the user that the Tweet has referenced"),
        bigquery.SchemaField("referenced_tweet_author_username", "STRING", mode="NULLABLE",
                             description="The username of the user that the Tweet has referenced"),
        bigquery.SchemaField("referenced_tweet_author_name", "STRING", mode="NULLABLE",
                             description="The name of the user that the Tweet has referenced"),
        bigquery.SchemaField("referenced_tweet_author_description", "STRING", mode="NULLABLE",
                             description="Text of the profile description of the user that the Tweet has referenced"),
        bigquery.SchemaField("referenced_tweet_author_url", "STRING", mode="NULLABLE",
                             description="The URL on the profile of the user that the Tweet has referenced"),
        bigquery.SchemaField("referenced_tweet_author_public_metrics_followers_count", "INTEGER",
                             mode="NULLABLE",
                             description="The number of entities following the user that the Tweet has referenced"),
        bigquery.SchemaField("referenced_tweet_author_public_metrics_following_count", "INTEGER",
                             mode="NULLABLE",
                             description="The number of entites the user that the Tweet has referenced is following"),
        bigquery.SchemaField("referenced_tweet_author_public_metrics_tweet_count", "INTEGER",
                             mode="NULLABLE",
                             description="The total number of Tweets posted by the user that the Tweet has referenced"),
        bigquery.SchemaField("referenced_tweet_author_public_metrics_listed_count", "INTEGER",
                             mode="NULLABLE",
                             description="The number of public lists the user that the Tweet has referenced is a member of"),
        bigquery.SchemaField("referenced_tweet_author_created_at", "TIMESTAMP", mode="NULLABLE",
                             description="When the account of the user that the Tweet has referenced was created (UTC)"),
        bigquery.SchemaField("referenced_tweet_author_location", "STRING", mode="NULLABLE",
                             description="Location of the user that the Tweet has referenced"),
        bigquery.SchemaField("referenced_tweet_author_pinned_tweet_id", "STRING", mode="NULLABLE",
                             description="ID of pinned Tweet of the user that the Tweet has referenced"),
        bigquery.SchemaField("referenced_tweet_author_profile_image_url", "STRING", mode="NULLABLE",
                             description="Profile image URL of the user that the Tweet has referenced"),
        bigquery.SchemaField("referenced_tweet_author_protected", "BOOL", mode="NULLABLE",
                             description="Whether or not the Tweets of the user that the Tweet has referenced are visible only to followers"),
        bigquery.SchemaField("referenced_tweet_author_verified", "BOOL", mode="NULLABLE",
                             description="Whether or not the account of the user that Tweet has referenced has been verified by Twitter"),
        bigquery.SchemaField("geo_id", "STRING", mode="NULLABLE",
                             description="*** Duplicate data - geo_place_id more consistent ***"),
        bigquery.SchemaField("geo_place_id", "STRING", mode="NULLABLE",
                             description="The unique identifier of the place, if this is a point of interest tagged in the Tweet"),
        bigquery.SchemaField("geo_name", "STRING", mode="NULLABLE",
                             description="Short human-readable representation of the place’s name, e.g. Brisbane"),
        bigquery.SchemaField("geo_country", "STRING", mode="NULLABLE",
                             description="Name of the country containing this place, e.g. Australia"),
        bigquery.SchemaField("geo_country_code", "STRING", mode="NULLABLE",
                             description="Shortened country code representing the country containing this place"),
        bigquery.SchemaField("geo_full_name", "STRING", mode="NULLABLE",
                             description="Full human-readable representation of the place’s name, e.g. Brisbane, Queensland"),
        bigquery.SchemaField("geo_geo_type", "STRING", mode="NULLABLE",
                             description="*** The type of ?? represented by this place, e.g. feature ***"),
        bigquery.SchemaField("geo_place_type", "STRING", mode="NULLABLE",
                             description="The type of location represented by this place, e.g. city"),
        bigquery.SchemaField("geo_geo_bbox", "STRING", mode="NULLABLE",
                             description="The type of location represented by this place"),
        bigquery.SchemaField("poll_id", "STRING", mode="NULLABLE",
                             description="Unique identifier of the expanded poll"),
        bigquery.SchemaField("poll_voting_status", "STRING", mode="NULLABLE",
                             description="Indicates if this poll is still active and can receive votes, or if the voting is now closed."),
        bigquery.SchemaField("poll_duration_minutes", "STRING", mode="NULLABLE",
                             description="Specifies the total duration of this poll"),
        bigquery.SchemaField("poll_end_datetime", "TIMESTAMP", mode="NULLABLE",
                             description="Specifies the end date and time for this pol"),
        bigquery.SchemaField("twarc_retrieved_at", "TIMESTAMP", mode="NULLABLE",
                             description="Date and time at which Twarc retrieved this Tweet"),
        bigquery.SchemaField("twarc_url", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("twarc_version", "STRING", mode="NULLABLE",
                             description="The version of Twarc that was used to retrieve this Tweet"),
    ]

    media_schema = [
        bigquery.SchemaField("tweet_id", "STRING", mode="REQUIRED",
                             description="Unique ID for this tweet (from Twitter)"),
        bigquery.SchemaField("media_height", "STRING", mode="NULLABLE",
                             description="Height of the attached media in pixels"),
        bigquery.SchemaField("media_key", "STRING", mode="NULLABLE",
                             description="Unique identifier of the expanded media content."),
        bigquery.SchemaField("media_width", "STRING", mode="NULLABLE",
                             description="Width of the attached media in pixels"),
        bigquery.SchemaField("media_type", "STRING", mode="NULLABLE",
                             description="Type of media content (image, video, animated GIF)"),
        bigquery.SchemaField("media_url", "STRING", mode="NULLABLE", description="URL for attached media"),
        bigquery.SchemaField("media_duration_ms", "STRING", mode="NULLABLE",
                             description="Duration of attached media in milliseconds, if video or animated GIF"),
        bigquery.SchemaField("media_preview_image_url", "STRING", mode="NULLABLE",
                             description="URL to the static placeholder preview of this content, if video or animated GIF"),
        bigquery.SchemaField("media_public_metrics_view_count", "INTEGER", mode="NULLABLE",
                             description="Count of views of media"),
        bigquery.SchemaField("media_alt_text", "STRING", mode="NULLABLE", description="")
    ]

    annotations_schema = [
        bigquery.SchemaField("tweet_id", "STRING", mode="REQUIRED",
                             description="Unique ID for this tweet (from Twitter)"),
        bigquery.SchemaField("tweet_annotation_start", "STRING", mode="NULLABLE",
                             description="The start position (zero-based) of the text used to annotate the Tweet. All start indices are inclusive"),
        bigquery.SchemaField("tweet_annotation_end", "STRING", mode="NULLABLE",
                             description="The end position (zero based) of the text used to annotate the Tweet. While all other end indices are exclusive, this one is inclusive"),
        bigquery.SchemaField("tweet_annotation_probability", "STRING", mode="NULLABLE",
                             description="The confidence score for the annotation as it correlates to the Tweet text"),
        bigquery.SchemaField("tweet_annotation_type", "STRING", mode="NULLABLE",
                             description="The description of the type of entity identified when the Tweet text was interpreted"),
        bigquery.SchemaField("tweet_annotation_normalized_text", "STRING", mode="NULLABLE",
                             description="The text used to determine the annotation type")
    ]

    context_annotations_schema = [
        bigquery.SchemaField("tweet_id", "STRING", mode="REQUIRED",
                             description="Unique ID for this Tweet (from Twitter)"),
        bigquery.SchemaField("tweet_context_annotation_domain_id", "STRING", mode="NULLABLE",
                             description="Contains the numeric value of the domain"),
        bigquery.SchemaField("tweet_context_annotation_domain_name", "STRING", mode="NULLABLE",
                             description="Domain name based on the Tweet text"),
        bigquery.SchemaField("tweet_context_annotation_domain_description", "STRING", mode="NULLABLE",
                             description="Long form description of domain classification"),
        bigquery.SchemaField("tweet_context_annotation_entity_id", "STRING", mode="NULLABLE",
                             description="Unique value which correlates to an explicitly mentioned Person, Place, Product or Organization"),
        bigquery.SchemaField("tweet_context_annotation_entity_name", "STRING", mode="NULLABLE",
                             description="Name or reference of entity referenced in the Tweet"),
        bigquery.SchemaField("tweet_context_annotation_entity_description", "STRING", mode="NULLABLE",
                             description="Additional information regarding referenced entity")
    ]

    hashtags_schema = [
        bigquery.SchemaField("tweet_id", "STRING", mode="REQUIRED",
                             description="Unique ID for this Tweet (from Twitter)"),
        bigquery.SchemaField("hashtags_start", "STRING", mode="NULLABLE",
                             description="The start position (zero-based) of the recognized Hashtag within the Tweet. All start indices are inclusive"),
        bigquery.SchemaField("hashtags_end", "STRING", mode="NULLABLE",
                             description="The end position (zero-based) of the recognized Hashtag within the Tweet. This end index is exclusive"),
        bigquery.SchemaField("hashtags_tag", "STRING", mode="NULLABLE",
                             description="The text of the hashtag within in the Tweet")
    ]

    urls_schema = [
        bigquery.SchemaField("tweet_id", "STRING", mode="REQUIRED",
                             description="Unique ID for this tweet (from Twitter)"),
        bigquery.SchemaField("urls_start", "STRING", mode="NULLABLE",
                             description="The start position (zero-based) of the recognized URL within the Tweet. All start indices are inclusive"),
        bigquery.SchemaField("urls_end", "STRING", mode="NULLABLE",
                             description="The end position (zero-based) of the recognized URL within the Tweet. This end index is exclusive"),
        bigquery.SchemaField("urls_url", "STRING", mode="NULLABLE",
                             description="The URL in the format tweeted by the user"),
        bigquery.SchemaField("urls_expanded_url", "STRING", mode="NULLABLE",
                             description="The fully resolved URL"),
        bigquery.SchemaField("urls_display_url", "STRING", mode="NULLABLE",
                             description="The URL as displayed in the Twitter client"),
        bigquery.SchemaField("urls_status", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("urls_unwound_url", "STRING", mode="NULLABLE",
                             description="The full destination URL"),
        bigquery.SchemaField("urls_images", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("urls_title", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("urls_description", "STRING", mode="NULLABLE", description="")
    ]

    mentions_schema = [
        bigquery.SchemaField("tweet_id", "STRING", mode="REQUIRED",
                             description="Unique ID for this tweet (from Twitter)"),
        bigquery.SchemaField("tweet_mentions_author_start", "STRING", mode="NULLABLE",
                             description="The start position (zero-based) of the recognized user mention within the Tweet. All start indices are inclusive"),
        bigquery.SchemaField("tweet_mentions_author_end", "STRING", mode="NULLABLE",
                             description="The end position (zero-based) of the recognized user mention within the Tweet. This end index is exclusive"),
        bigquery.SchemaField("tweet_mentions_author_username", "STRING", mode="NULLABLE",
                             description="The part of text recognized as a user mention"),
        bigquery.SchemaField("tweet_mentions_author_id", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("tweet_mentions_author_protected", "BOOL", mode="NULLABLE", description=""),
        bigquery.SchemaField("tweet_mentions_author_location", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("tweet_mentions_author_verified", "BOOL", mode="NULLABLE", description=""),
        bigquery.SchemaField("tweet_mentions_author_profile_image_url", "STRING", mode="NULLABLE",
                             description=""),
        bigquery.SchemaField("tweet_mentions_author_description", "STRING", mode="NULLABLE",
                             description=""),
        bigquery.SchemaField("tweet_mentions_author_created_at", "TIMESTAMP", mode="NULLABLE",
                             description=""),
        bigquery.SchemaField("tweet_mentions_author_pinned_tweet_id", "STRING", mode="NULLABLE",
                             description=""),
        bigquery.SchemaField("tweet_mentions_author_url", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("tweet_mentions_author_name", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("tweet_mentions_author_public_metrics_followers_count", "INTEGER",
                             mode="NULLABLE",
                             description=""),
        bigquery.SchemaField("tweet_mentions_author_public_metrics_following_count", "INTEGER",
                             mode="NULLABLE",
                             description=""),
        bigquery.SchemaField("tweet_mentions_author_public_metrics_tweet_count", "INTEGER", mode="NULLABLE",
                             description=""),
        bigquery.SchemaField("tweet_mentions_author_public_metrics_listed_count", "INTEGER",
                             mode="NULLABLE",
                             description="")
    ]

    author_description_schema = [
        bigquery.SchemaField("author_id", "STRING", mode="NULLABLE",
                             description="Unique ID for this author (from Twitter)"),
        bigquery.SchemaField("author_description_hashtags_start", "STRING", mode="NULLABLE",
                             description=""),
        bigquery.SchemaField("author_description_hashtags_end", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("author_description_hashtags_tag", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("author_description_mentions_start", "STRING", mode="NULLABLE",
                             description=""),
        bigquery.SchemaField("author_description_mentions_end", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("author_description_mentions_username", "STRING", mode="NULLABLE",
                             description=""),
        bigquery.SchemaField("author_description_urls_start", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("author_description_urls_end", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("author_description_urls_url", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("author_description_urls_expanded_url", "STRING", mode="NULLABLE",
                             description=""),
        bigquery.SchemaField("author_description_urls_display_url", "STRING", mode="NULLABLE",
                             description="")
    ]

    author_urls_shema = [
        bigquery.SchemaField("author_id", "STRING", mode="REQUIRED",
                             description="Unique ID for this author (from Twitter)"),
        bigquery.SchemaField("author_start", "STRING", mode="NULLABLE",
                             description=""),
        bigquery.SchemaField("author_end", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("author_url", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("author_expanded_url", "STRING", mode="NULLABLE",
                             description=""),
        bigquery.SchemaField("author_display_url", "STRING", mode="NULLABLE",
                             description="")
    ]

    poll_schema = [
        bigquery.SchemaField("poll_id", "STRING", mode="REQUIRED",
                             description="Unique ID for this poll (from Twitter)"),
        bigquery.SchemaField("poll_position", "STRING", mode="NULLABLE", description="Poll option"),
        bigquery.SchemaField("poll_label", "STRING", mode="NULLABLE", description="Poll label"),
        bigquery.SchemaField("poll_votes", "INTEGER", mode="NULLABLE",
                             description="Count of votes for poll option")
    ]

    interactions_schema = [
        bigquery.SchemaField("tweet_id", "STRING", mode="REQUIRED",
                             description="Unique ID for tweet"),
        bigquery.SchemaField("tweet_type", "STRING", mode="NULLABLE",
                             description="Unique ID for this poll (from Twitter)"),
        bigquery.SchemaField("to_user_id", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("to_user_username", "STRING", mode="NULLABLE", description="")
    ]

    edit_history_schema = [
        bigquery.SchemaField("tweet_id", "STRING", mode="REQUIRED",
                             description="Unique ID for tweet"),
        bigquery.SchemaField("edit_history_tweet_ids", "STRING", mode="NULLABLE",
                             description="Unique ID for edited Tweet")
    ]

    list_of_csv = ['TWEETS.csv',
                   'MEDIA.csv',
                   'ANNOTATIONS.csv',
                   'CONTEXT_ANNOTATIONS.csv',
                   'HASHTAGS.csv',
                   'URLS.csv',
                   'MENTIONS.csv',
                   'AUTHOR_DESCRIPTION.csv',
                   'AUTHOR_URLS.csv',
                   'POLL_OPTIONS.csv',
                   'INTERACTIONS.csv',
                   'EDIT_HISTORY.csv']

    list_of_schema = [tweet_schema,
                      media_schema,
                      annotations_schema,
                      context_annotations_schema,
                      hashtags_schema,
                      urls_schema,
                      mentions_schema,
                      author_description_schema,
                      author_urls_shema,
                      poll_schema,
                      interactions_schema,
                      edit_history_schema]

    list_of_tablenames = ['tweets',
                          'media',
                          'annotations',
                          'context_annotations',
                          'hashtags',
                          'urls',
                          'mentions',
                          'author_description',
                          'author_urls',
                          'poll_options',
                          'interactions',
                          'edit_history']

class TCAT_schema:

    tweet_schema = [
        bigquery.SchemaField("id", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("time", "TIMESTAMP", mode="NULLABLE", description=""),
        bigquery.SchemaField("created_at", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("from_user_name", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("text", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("filter_level", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("possibly_sensitive", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("withheld_copyright", "INTEGER", mode="NULLABLE", description=""),
        bigquery.SchemaField("withheld_scope", "INTEGER", mode="NULLABLE", description=""),
        bigquery.SchemaField("truncated", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("retweet_count", "INTEGER", mode="NULLABLE", description=""),
        bigquery.SchemaField("favorite_count", "INTEGER", mode="NULLABLE", description=""),
        bigquery.SchemaField("lang", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("to_user_name", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("in_reply_to_status_id", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("quoted_status_id", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("source", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("location", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("lat", "FLOAT", mode="NULLABLE", description=""),
        bigquery.SchemaField("lng", "FLOAT", mode="NULLABLE", description=""),
        bigquery.SchemaField("from_user_id", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("from_user_realname", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("from_user_verified", "BOOLEAN", mode="NULLABLE", description=""),
        bigquery.SchemaField("from_user_description", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("from_user_url", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("from_user_profile_image_url", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("from_user_utcoffset", "INTEGER", mode="NULLABLE", description=""),
        bigquery.SchemaField("from_user_timezone", "INTEGER", mode="NULLABLE", description=""),
        bigquery.SchemaField("from_user_lang", "INTEGER", mode="NULLABLE", description=""),
        bigquery.SchemaField("from_user_tweetcount", "INTEGER", mode="NULLABLE", description=""),
        bigquery.SchemaField("from_user_followercount", "INTEGER", mode="NULLABLE", description=""),
        bigquery.SchemaField("from_user_friendcount", "INTEGER", mode="NULLABLE", description=""),
        bigquery.SchemaField("from_user_favourites_count", "INTEGER", mode="NULLABLE", description=""),
        bigquery.SchemaField("from_user_listed", "INTEGER", mode="NULLABLE", description=""),
        bigquery.SchemaField("from_user_withheld_scope", "INTEGER", mode="NULLABLE", description=""),
        bigquery.SchemaField("from_user_created_at", "STRING", mode="NULLABLE", description="")
    ]

    hashtags_schema = [
        bigquery.SchemaField("tweet_id", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("hashtag", "STRING", mode="NULLABLE", description="")
    ]

    mentions_schema = [
        bigquery.SchemaField("tweet_id", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("user_from_id", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("user_from_name", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("user_to_id", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("user_to_name", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("mention_type", "STRING", mode="NULLABLE", description="")
    ]

    list_of_csv = ['TWEETS.csv',
                   'HASHTAGS.csv',
                   'MENTIONS.csv']

    list_of_schema = [tweet_schema,
                      hashtags_schema,
                      mentions_schema]

    list_of_tablenames = ['tweets',
                          'hashtags',
                          'mentions']

class TweetQuery_schema:

    tweets_flat_schema = [
        bigquery.SchemaField("coordinates_coordinates_0", "FLOAT", mode="NULLABLE", description=""),
        bigquery.SchemaField("coordinates_coordinates_1", "FLOAT", mode="NULLABLE", description=""),
        bigquery.SchemaField("coordinates_type", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("created_at", "TIMESTAMP", mode="NULLABLE", description=""),
        bigquery.SchemaField("entities_polls", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("entities_symbols", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("favorite_count", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("favorited", "BOOLEAN", mode="NULLABLE", description=""),
        bigquery.SchemaField("filter_level", "BOOLEAN", mode="NULLABLE", description=""),
        bigquery.SchemaField("full_text", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("geo_coordinates_0", "FLOAT", mode="NULLABLE", description=""),
        bigquery.SchemaField("geo_coordinates_1", "FLOAT", mode="NULLABLE", description=""),
        bigquery.SchemaField("geo_type", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("id", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("in_reply_to_screen_name", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("in_reply_to_status_id", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("in_reply_to_user_id", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("is_quote_status", "BOOLEAN", mode="NULLABLE", description=""),
        bigquery.SchemaField("lang", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("matching_rules", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("place_country", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("place_country_code", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("place_full_name", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("place_id", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("place_name", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("place_place_type", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("place_url", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("possibly_sensitive", "BOOLEAN", mode="NULLABLE", description=""),
        bigquery.SchemaField("quote_count", "INTEGER", mode="NULLABLE", description=""),
        bigquery.SchemaField("quoted_status_id", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("quoted_status_text", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("quoted_status_user_id", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("reply_count", "INTEGER", mode="NULLABLE", description=""),
        bigquery.SchemaField("retweet_count", "INTEGER", mode="NULLABLE", description=""),
        bigquery.SchemaField("retweeted", "BOOLEAN", mode="NULLABLE", description=""),
        bigquery.SchemaField("retweeted_status_id", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("retweeted_status_user_id", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("source", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("text", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("truncated", "BOOLEAN", mode="NULLABLE", description=""),
        bigquery.SchemaField("user_contributors_enabled", "BOOLEAN", mode="NULLABLE", description=""),
        bigquery.SchemaField("user_created_at", "TIMESTAMP", mode="NULLABLE", description=""),
        bigquery.SchemaField("user_default_profile", "BOOLEAN", mode="NULLABLE", description=""),
        bigquery.SchemaField("user_default_profile_image", "BOOLEAN", mode="NULLABLE", description=""),
        bigquery.SchemaField("user_description", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("user_favourites_count", "INTEGER", mode="NULLABLE", description=""),
        bigquery.SchemaField("user_followers_count", "INTEGER", mode="NULLABLE", description=""),
        bigquery.SchemaField("user_friends_count", "INTEGER", mode="NULLABLE", description=""),
        bigquery.SchemaField("user_geo_enabled", "BOOLEAN", mode="NULLABLE", description=""),
        bigquery.SchemaField("user_id", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("user_is_translator", "BOOLEAN", mode="NULLABLE", description=""),
        bigquery.SchemaField("user_lang", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("user_listed_count", "INTEGER", mode="NULLABLE", description=""),
        bigquery.SchemaField("user_location", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("user_name", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("user_profile_background_color", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("user_profile_background_image_url", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("user_profile_background_title", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("user_profile_banner_url", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("user_profile_fill_color", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("user_profile_image_url", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("user_profile_image_url_https", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("user_profile_link_color", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("user_profile_sidebar_border_color", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("user_profile_text_color", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("user_profile_use_background_image", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("user_screen_name", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("user_statuses_count", "INTEGER", mode="NULLABLE", description=""),
        bigquery.SchemaField("user_time_zone", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("user_url", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("user_utc_offset", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("user_verified", "BOOLEAN", mode="NULLABLE", description=""),
        bigquery.SchemaField("entities_hashtags_text", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("entities_user_mentions_name", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("entities_user_mention_id", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("entities_user_mention_screen_name", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("urls_url", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("urls_expanded_url", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("urls_unshortened_url", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("urls_domain_path", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("urls_status", "STRING", mode="NULLABLE", description="")
    ]

    list_of_csv = ['TWEETS_flat.csv']

    list_of_schema = [tweets_flat_schema]

    list_of_tablenames = ['tweets_flat']

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
            try:
                # For test, this will throw exception because no counts are run; convert to None
                tweet_count = len(list_of_dataframes[0].loc[list_of_dataframes[0]['reference_level'] == '0']) + tweet_count
            except:
                tweet_count = None
        elif Schematype.TCAT == True:
            list_of_dataframes = transform_schema.transform_DATA_to_TCAT(list_of_dataframes)
            list_of_csv = TCAT_schema.list_of_csv
            list_of_tablenames = TCAT_schema.list_of_tablenames
            list_of_schema = TCAT_schema.list_of_schema
            try:
                tweet_count = len(list_of_dataframes[0]) + tweet_count
            except:
                tweet_count = None
        else:
            list_of_dataframes = transform_schema.transform_DATA_to_TQ(list_of_dataframes)
            list_of_csv = TweetQuery_schema.list_of_csv
            list_of_tablenames = TweetQuery_schema.list_of_tablenames
            list_of_schema = TweetQuery_schema.list_of_schema
            try:
                tweet_count = len(list_of_dataframes[0].drop_duplicates(subset=['id'])) + tweet_count
            except:
                tweet_count = None

        return list_of_dataframes, list_of_csv, list_of_tablenames, list_of_schema, tweet_count

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