# This file contains column order lists and dtype dictionaries for fields to match table schema

# TABLE FIELDS
# ----------------------------------------------------------------------------------------------------------------------
class DATA_fields:
    tweet_column_order = [
        'tweet_id',
        'tweet_text',
        'tweet_type',
        'created_at',
        'public_metrics_like_count',
        'public_metrics_quote_count',
        'public_metrics_reply_count',
        'public_metrics_retweet_count',
        'is_retweet',
        'is_reply',
        'is_quote',
        'has_mention',
        'has_media',
        'has_hashtags',
        'has_urls',
        'has_annotations',
        'has_context_annotations',
        'lang',
        'possibly_sensitive',
        'conversation_id',
        'reply_settings',
        'source',
        'is_referenced',
        'reference_level',
        'referencing_tweet_id',
        'referenced_tweet_id',
        'author_id',
        'author_username',
        'author_name',
        'author_description',
        'author_url',
        'author_public_metrics_followers_count',
        'author_public_metrics_following_count',
        'author_public_metrics_tweet_count',
        'author_public_metrics_listed_count',
        'author_created_at',
        'author_location',
        'author_pinned_tweet_id',
        'author_profile_image_url',
        'author_protected',
        'author_verified',
        'referenced_tweet_text',
        'referenced_tweet_author_id',
        'referenced_tweet_author_username',
        'referenced_tweet_author_name',
        'referenced_tweet_author_description',
        'referenced_tweet_author_url',
        'referenced_tweet_author_public_metrics_followers_count',
        'referenced_tweet_author_public_metrics_following_count',
        'referenced_tweet_author_public_metrics_listed_count',
        'referenced_tweet_author_public_metrics_tweet_count',
        'referenced_tweet_author_created_at',
        'referenced_tweet_author_location',
        'referenced_tweet_author_pinned_tweet_id',
        'referenced_tweet_author_profile_image_url',
        'referenced_tweet_author_protected',
        'referenced_tweet_author_verified',
        'geo_id',
        'geo_place_id',
        'geo_name',
        'geo_country',
        'geo_country_code',
        'geo_full_name',
        'geo_geo_type',
        'geo_place_type',
        'geo_geo_bbox',
        'attachments_poll_id',
        'attachments_poll_voting_status',
        'attachments_poll_duration_minutes',
        'attachments_poll_end_datetime',
        'twarc_retrieved_at',
        'twarc_url',
        'twarc_version'
    ]

    hashtags_column_order = [
        'tweet_id',
        'hashtags_start',
        'hashtags_end',
        'hashtags_tag'
    ]

    urls_column_order = [
        'tweet_id',
        'urls_start',
        'urls_end',
        'urls_url',
        'urls_expanded_url',
        'urls_display_url',
        'urls_status',
        'urls_unwound_url',
        'urls_images',
        'urls_title',
        'urls_description'
    ]

    mention_column_order = [
        'tweet_id',
        'tweet_mentions_author_start',
        'tweet_mentions_author_end',
        'tweet_mentions_author_username',
        'tweet_mentions_author_id',
        'tweet_mentions_author_protected',
        'tweet_mentions_author_location',
        'tweet_mentions_author_verified',
        'tweet_mentions_author_profile_image_url',
        'tweet_mentions_author_description',
        'tweet_mentions_author_created_at',
        'tweet_mentions_author_pinned_tweet_id',
        'tweet_mentions_author_url',
        'tweet_mentions_author_name',
        'tweet_mentions_author_public_metrics_followers_count',
        'tweet_mentions_author_public_metrics_following_count',
        'tweet_mentions_author_public_metrics_tweet_count',
        'tweet_mentions_author_public_metrics_listed_count'
    ]

    media_column_order = [
        'tweet_id',
        'media_height',
        'media_key',
        'media_width',
        'media_type',
        'media_url',
        'media_duration_ms',
        'media_preview_image_url',
        'media_public_metrics_view_count',
        'media_alt_text'
    ]

    poll_options_column_order = [
        'poll_id',
        'poll_position',
        'poll_label',
        'poll_votes'
    ]

    author_description_column_order = [
        'author_id',
        'author_description_hashtags_start',
        'author_description_hashtags_end',
        'author_description_hashtags_tag',
        'author_description_mentions_start',
        'author_description_mentions_end',
        'author_description_mentions_username',
        'author_description_urls_start',
        'author_description_urls_end',
        'author_description_urls_url',
        'author_description_urls_expanded_url',
        'author_description_urls_display_url'
    ]

    type_list = ['author', 'tweet_mentions_author', 'in_reply_to_user']


    author_desc_hashtags_cols = [
        '_id',
        '_hashtags_start',
        '_hashtags_end',
        '_hashtags_tag'
    ]

    author_desc_mentions_cols = [
        '_mentions_start',
        '_mentions_end',
        '_mentions_username'
    ]

    author_desc_urls_cols = [
        '_urls_start',
        '_urls_end',
        '_urls_url',
        '_urls_expanded_url',
        '_urls_display_url'
    ]

    # author_description_hashtags_column_order = [
    #     'author_id',
    #     'author_description_hashtags_start',
    #     'author_description_hashtags_end',
    #     'author_description_hashtags_tag',
    #     'author_description_mentions_start',
    #     'author_description_mentions_end',
    #     'author_description_mentions_username',
    #     'author_description_urls_start',
    #     'author_description_urls_end',
    #     'author_description_urls_url',
    #     'author_description_urls_expanded_url',
    #     'author_description_urls_display_url'
    # ]
    #
    # author_description_mentions_column_order = [
    #     'tweet_mentions_author_id',
    #     'mentioned_author_description_hashtags_start',
    #     'mentioned_author_description_hashtags_end',
    #     'mentioned_author_description_hashtags_tag',
    #     'mentioned_author_description_mentions_start',
    #     'mentioned_author_description_mentions_end',
    #     'mentioned_author_description_mentions_username',
    #     'mentioned_author_description_urls_start',
    #     'mentioned_author_description_urls_end',
    #     'mentioned_author_description_urls_url',
    #     'mentioned_author_description_urls_expanded_url',
    #     'mentioned_author_description_urls_display_url'
    # ]
    #
    # author_description_urls_column_order = [
    #     'author_id',
    #     'author_description_hashtags_start',
    #     'author_description_hashtags_end',
    #     'author_description_hashtags_tag',
    #     'author_description_mentions_start',
    #     'author_description_mentions_end',
    #     'author_description_mentions_username',
    #     'author_description_urls_start',
    #     'author_description_urls_end',
    #     'author_description_urls_url',
    #     'author_description_urls_expanded_url',
    #     'author_description_urls_display_url'
    # ]


    author_urls_column_order = [
        'author_id',
        'author_url_start',
        'author_url_end',
        'author_url',
        'author_expanded_url',
        'author_display_url'
    ]

    interactions_column_order = [
        'tweet_id',
        'tweet_type',
        'to_user_id',
        'to_user_username'
    ]

    annotations_column_order = [
        'tweet_id',
        'tweet_annotation_start',
        'tweet_annotation_end',
        'tweet_annotation_probability',
        'tweet_annotation_type',
        'tweet_annotation_normalized_text'
    ]

    context_annotations_column_order = [
        'tweet_id',
        'tweet_context_annotation_domain_id',
        'tweet_context_annotation_domain_name',
        'tweet_context_annotation_domain_description',
        'tweet_context_annotation_entity_id',
        'tweet_context_annotation_entity_name',
        'tweet_context_annotation_entity_description'
    ]

    edit_history = [
        'tweet_id',
        'edit_history_tweet_ids'
    ]

    up_a_level_column_list = [
        'tweet_id',
        'referencing_tweet_id',
        'type',
        'text',
        'entities.hashtags',
        'entities.mentions',
        'entities.urls',
        'entities.annotations',
        'author.id',
        'author.name',
        'author.username',
        'author.description',
        'author.url',
        'author.public_metrics.followers_count',
        'author.public_metrics.following_count',
        'author.public_metrics.tweet_count',
        'author.public_metrics.listed_count',
        'author.created_at',
        'author.location',
        'author.pinned_tweet_id',
        'author.profile_image_url',
        'author.protected',
        'author.verified']


    dfs_move_up_colnames = {
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
        'author.verified': 'referenced_tweet_author_verified'}


    col_to_bool_list = [
        'attachments_media',
        'entities_mentions',
        'entities_hashtags',
        'entities_urls',
        'entities_annotations',
        'context_annotations']

    bool_has_list = [
        'has_media',
        'has_mention',
        'has_hashtags',
        'has_urls',
        'has_annotations',
        'has_context_annotations']

    boolean_cols = [
        'possibly_sensitive',
        'author_protected',
        'author_verified',
        'referenced_tweet_author_protected',
        'referenced_tweet_author_verified',
        'is_reply',
        'is_quote',
        'is_retweet',
        'has_mention',
        'has_media',
        'has_hashtags',
        'has_urls',
        'has_annotations',
        'has_context_annotations']


    # DTYPE DICTS
    # ----------------------------------------------------------------------------------------------------------------------
    tweet_table_dtype_dict = {
        'tweet_id': 'string',
        'referenced_tweet_id': 'string',
        'referencing_tweet_id': 'string',
        'conversation_id': 'string',
        'author_id': 'string',
        'author_pinned_tweet_id': 'string',
        'referenced_tweet_author_id': 'string',
        'referenced_tweet_author_pinned_tweet_id': 'string',
        'geo_id': 'string',
        'geo_place_id': 'string',
        'public_metrics_like_count': 'int32',
        'public_metrics_quote_count': 'int32',
        'public_metrics_reply_count': 'int32',
        'public_metrics_retweet_count': 'int32',
        'author_public_metrics_followers_count': 'int32',
        'author_public_metrics_following_count': 'int32',
        'author_public_metrics_tweet_count': 'int32',
        'author_public_metrics_listed_count': 'int32',
        'referenced_tweet_author_public_metrics_followers_count': 'int32',
        'referenced_tweet_author_public_metrics_following_count': 'int32',
        'referenced_tweet_author_public_metrics_listed_count': 'int32',
        'referenced_tweet_author_public_metrics_tweet_count': 'int32'}


class TCAT_fields:

    tweet_column_order = [
        'tweet_id',
        'time',
        'created_at',
        'author_username',
        'tweet_text',
        'filter_level',
        'possibly_sensitive',
        'withheld_copyright',
        'withheld_scope',
        'truncated',
        'public_metrics_retweet_count',
        'public_metrics_like_count',
        'lang',
        'referenced_tweet_author_username',
        'in_reply_to_status_id',
        'quoted_status_id',
        'source',
        'author_location',
        'lat',
        'lng',
        'author_id',
        'author_name',
        'author_verified',
        'author_description',
        'author_url',
        'author_profile_image_url',
        'from_user_utcoffset',
        'from_user_timezone',
        'from_user_lang',
        'author_public_metrics_tweet_count',
        'author_public_metrics_followers_count',
        'author_public_metrics_following_count',
        'from_user_favourites_count',
        'author_public_metrics_listed_count',
        'from_user_withheld_scope',
        'author_created_at']

    hashtags_column_order = [
        'tweet_id',
        'hashtags_tag'
    ]

    # mentions_column_order = [
    #     'tweet_id',
    #     'author_id',
    #     'author_username',
    #     'tweet_mentions_author_id',
    #     'tweet_mentions_author_username',
    #     'tweet_type'
    # ]

    MENTIONS_column_order = [
        'tweet_id',
        'user_from_id',
        'user_from_name',
        'user_to_id',
        'user_to_name',
        'mention_type'

    ]

    tweet_column_names_dict = {
        'tweet_id':'id',
        'author_username':'from_user_name',
        'tweet_text':'text',
        'public_metrics_retweet_count':'retweet_count',
        'public_metrics_like_count':'like_count',
        'referenced_tweet_author_username':'to_user_name',
        'author_location':'location',
        'author_id':'from_user_id',
        'author_name':'from_user_realname',
        'author_verified':'from_user_verified',
        'author_description':'from_user_description',
        'author_url':'from_user_url',
        'author_profile_image_url':'from_user_profile_image_url',
        'author_public_metrics_tweet_count':'from_user_tweetcount',
        'author_public_metrics_followers_count':'from_user_followercount',
        'author_public_metrics_following_count':'from_user_friendcount',
        'author_public_metrics_listed_count':'from_user_listed',
        'author_created_at':'from_user_created_at'}

    hashtags_column_names_dict = {
        'hashtags_tag':'hashtag'}

    mentions_column_names_dict = {
        'user_from_id': 'user_from_id',
        'user_from_username': 'user_from_name',
        'user_to_id':'user_to_id',
        'user_to_username':'user_to_name',
        'tweet_type': 'mention_type'}

    interactions_mentions_column_names_dict = {
        'referenced_tweet_author_id': 'tweet_mentions_author_id',
        'referenced_tweet_author_username': 'tweet_mentions_author_username'}

    interactions_to_column_names_dict = {
        'to_user_id': 'user_to_id',
        'to_user_username': 'user_to_name'
    }

    interactions_from_column_names_dict = {
        'author_id': 'user_from_id',
        'author_username': 'user_from_name'
    }

    blank_cols = [
        'filter_level',
        'withheld_copyright',
        'withheld_scope',
        'truncated',
        'lat',
        'lng',
        'from_user_utcoffset',
        'from_user_timezone',
        'from_user_lang',
        'from_user_favourites_count',
        'from_user_withheld_scope']


class TweetQuery_fields:

    tweet_column_order = [
        'coordinates_coordinates_0',
        'coordinates_coordinates_1',
        'coordinates_type',
        'created_at',
        'attachments_poll_id',
        'entities_symbols',
        'public_metrics_like_count',
        'favorited',
        'filter_level',
        'tweet_text',
        'geo_coordinates_0',
        'geo_coordinates_1',
        'geo_type',
        'tweet_id',
        'in_reply_to_screen_name',
        'in_reply_to_status_id',
        'in_reply_to_user_id',
        'is_quote_status',
        'lang',
        'matching_rules',
        'geo_country',
        'geo_country_code',
        'geo_full_name',
        'geo_id',
        'geo_name',
        'geo_place_type',
        'place_url',
        'possibly_sensitive',
        'public_metrics_quote_count',
        'quoted_status_id',
        'quoted_status_text',
        'quoted_status_user_id',
        'public_metrics_reply_count',
        'public_metrics_retweet_count',
        'retweeted',
        'retweeted_status_id',
        'retweeted_status_user_id',
        'source',
        'tweet_text',
        'truncated',
        'user_contributors_enabled',
        'author_created_at',
        'user_default_profile',
        'user_default_profile_image',
        'author_description',
        'user_favourites_count',
        'author_public_metrics_followers_count',
        'author_public_metrics_following_count',
        'user_geo_enabled',
        'author_id',
        'user_is_translator',
        'user_lang',
        'author_public_metrics_listed_count',
        'author_location',
        'author_name',
        'user_profile_background_color',
        'user_profile_background_image_url',
        'user_profile_background_title',
        'user_profile_banner_url',
        'user_profile_fill_color',
        'author_profile_image_url',
        'user_profile_image_url_https',
        'user_profile_link_color',
        'user_profile_sidebar_border_color',
        'user_profile_text_color',
        'user_profile_use_background_image',
        'author_username',
        'author_public_metrics_tweet_count',
        'user_time_zone',
        'author_url',
        'user_utc_offset',
        'author_verified',
        'entities_hashtags_text',
        'entities_user_mentions_name',
        'entities_user_mention_id',
        'entities_user_mention_screen_name',
        'urls_url',
        'urls_expanded_url',
        'urls_unshortened_url',
        'urls_domain_path',
        'urls_status'
    ]

    hashtags_column_order = [
        'tweet_id',
        'hashtags_tag'
    ]

    mentions_column_order = [
        'tweet_id',
        'tweet_mentions_author_name',
        'tweet_mentions_author_id',
        'tweet_mentions_author_username'
    ]

    urls_column_order = [
        'tweet_id',
        'urls_url',
        'urls_expanded_url',
        'urls_unshortened_url',
        'urls_domain_path',
        'urls_status'
    ]

    tweet_column_names_dict = {
        'tweet_id':'id',
        'tweet_text':'text',
        'attachments_poll_id':'entities_polls',
        'public_metrics_like_count':'favorite_count',
        'geo_country':'place_country',
        'geo_country_code':'place_country_code',
        'geo_full_name':'place_full_name',
        'geo_place_id':'place_id',
        'geo_name':'place_name',
        'geo_place_type':'place_place_type',
        'public_metrics_quote_count':'quote_count',
        'public_metrics_reply_count':'reply_count',
        'author_created_at':'user_created_at',
        'author_description':'user_description',
        'author_public_metrics_followers_count':'user_followers_count',
        'author_public_metrics_following_count':'user_friends_count',
        'author_id':'user_id',
        'author_public_metrics_listed_count':'user_listed_count',
        'author_location':'user_location',
        'author_name':'user_name',
        'author_profile_image_url':'user_profile_image_url',
        'author_username':'user_screen_name',
        'author_public_metrics_tweet_count':'user_statuses_count',
        'author_url':'user_url',
        'author_verified':'user_verified'}

    hashtags_column_names_dict = {
        'hashtags_tag': 'entities_hashtags_text'}

    mentions_column_names_dict = {
        'author_id': 'user_from_id',
        'author_username': 'user_from_name',
        'tweet_mentions_author_name': 'entities_user_mentions_name',
        'tweet_mentions_author_id': 'entities_user_mention_id',
        'tweet_mentions_author_username': 'entities_user_mention_screen_name',
        'tweet_type': 'mention_type'}

    MENTIONS_column_order = [
        'tweet_id',
        'entities_user_mentions_name',
        'entities_user_mention_id',
        'entities_user_mention_screen_name',
        'mention_type']

    # interactions_mentions_column_names_dict = {
    #     'referenced_tweet_author_name':'tweet_mentions_author_name',
    #     'referenced_tweet_author_id': 'tweet_mentions_author_id',
    #     'referenced_tweet_author_username': 'tweet_mentions_author_username'}

    blank_cols = [
        'coordinates_coordinates_0',
        'coordinates_coordinates_1',
        'coordinates_type',
        'entities_symbols',
        'favorited',
        'filter_level',
        'geo_coordinates_0',
        'geo_coordinates_1',
        'geo_type',
        'matching_rules',
        'place_url',
        'truncated',
        'user_contributors_enabled',
        'user_default_profile',
        'user_default_profile_image',
        'user_favourites_count',
        'user_geo_enabled',
        'user_is_translator',
        'user_lang',
        'user_profile_background_color',
        'user_profile_background_image_url',
        'user_profile_background_title',
        'user_profile_banner_url',
        'user_profile_fill_color',
        'user_profile_link_color',
        'user_profile_sidebar_border_color',
        'user_profile_text_color',
        'user_profile_use_background_image',
        'user_time_zone',
        'user_utc_offset']

    interactions_mentions_column_names_dict = {
        'referenced_tweet_author_id': 'tweet_mentions_author_id',
        'referenced_tweet_author_username': 'tweet_mentions_author_username'}

    interactions_to_column_names_dict = {
        'tweet_type': 'mention_type',
        'to_user_id': 'entities_user_mention_id',
        'to_user_username': 'entities_user_mention_screen_name',
        'tweet_mentions_author_name': 'entities_user_mentions_name'
    }
