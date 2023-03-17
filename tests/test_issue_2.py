from io import StringIO, BytesIO
import pytest
import pandas as pd
import json

TEST_ENCODINGS = ['utf-8', 'utf-16', 'utf-32']

@pytest.fixture
def three_tweets_with_breaks():
    tweet = """
        {"text": "normal \r\ntweet\r with\n \r #hashtag"}
        {"text": "roles.\rtext\r#Chashtag"}
        {"text": "normal tweet\r\n without\n\r \n \rhashtag"}
    """
    yield StringIO(tweet)

@pytest.fixture
def json_issue_2_minimal():
    tweet = """
        {"text": "normal \r\ntweet\r with\n \r #hashtag"}
        {"text": "roles.\rtext\ud83d\ude02\ud83d\ude02\r#Chashtag"}
        {"text": "normal LOVE LOVE him\ud83d\ude02\ud83d\ude02\r#CharlieAn\rhashtag"}
    """
    yield StringIO(tweet)

@pytest.fixture
def json_issue_2_minimal_binary():
    tweet = b"""\uFEFF
                {"text": "normal \r\ntweet\r with\n \r #hashtag"}
                {"text": "roles.\rtext\ud83d\ude02\ud83d\ude02\r#Chashtag"}
                {"text": "normal tweet\r\n without\n\r \n \rhashtag"}
                """
    yield BytesIO(tweet)

@pytest.fixture
def json_issue_2_complete():
    tweet = """{"context_annotations": [{"domain": {"id": "86", "name": "Movie", "description": "A film like Rogue One: A Star Wars Story"}, "entity": {"id": "1397956463998369794", "name": "Charlie and the Chocolate Factory"}}, 
    {"domain": {"id": "131", "name": "Unified Twitter Taxonomy", "description": "A taxonomy of user interests. "}, "entity": {"id": "1303715075421646849", "name": "Actors"}}, {"domain": {"id": "10", "name": "Person", 
    "description": "Named people in the world like Nelson Mandela"}, "entity": {"id": "878236727621459968", "name": "Johnny Depp", "description": "Johnny Depp"}}, {"domain": {"id": "56", "name": "Actor", 
    "description": "An actor or actress in the world, like Kate Winslet or Leonardo DiCaprio"}, "entity": {"id": "878236727621459968", "name": "Johnny Depp", "description": "Johnny Depp"}}, {"domain": 
    {"id": "131", "name": "Unified Twitter Taxonomy", "description": "A taxonomy of user interests. "}, "entity": {"id": "878236727621459968", "name": "Johnny Depp", "description": "Johnny Depp"}}, {"domain": 
    {"id": "131", "name": "Unified Twitter Taxonomy", "description": "A taxonomy of user interests. "}, "entity": {"id": "1095352268046495745", "name": "Celebrities", "description": "Celebrities"}}], "entities": 
    {"hashtags": [{"start": 97, "end": 127, "tag": "CharlieAndTheChocolateFactory"}], "mentions": [{"start": 3, "end": 12, "username": "Babygiwa", "id": "1043387532", 
    "profile_image_url": "https://pbs.twimg.com/profile_images/1633209667852681220/aUb4JXsD_normal.jpg", "pinned_tweet_id": "1119867901283053569", "entities": {"url": {"urls": [{"start": 0, "end": 23, "url": "https://t.co/2qVS2yErtW", 
    "expanded_url": "http://glittersushering.com", "display_url": "glittersushering.com"}]}}, "url": "https://t.co/2qVS2yErtW", "verified": false, "name": "Yemi", "description": "Living my best life.", "protected": false, 
    "public_metrics": {"followers_count": 10738, "following_count": 1287, "tweet_count": 353943, "listed_count": 113}, "location": "A place called forward ", "created_at": "2012-12-28T23:37:33.000Z"}], 
    "annotations": [{"start": 14, "end": 24, "probability": 0.9762, "type": "Person", "normalized_text": "Johnny Depp"}, {"start": 98, "end": 126, "probability": 0.7921, "type": "Person", "normalized_text": "CharlieAndTheChocolateFactory"}]}, 
    "lang": "en", "reply_settings": "everyone", "public_metrics": {"retweet_count": 2, "reply_count": 0, "like_count": 0, "quote_count": 0, "impression_count": 0}, "conversation_id": "1517074359571075072", "id": "1517074359571075072", 
    "possibly_sensitive": false, "author_id": "1043387532", "edit_history_tweet_ids": ["1517074359571075072"], "referenced_tweets": [{"type": "retweeted", "id": "688089054999633920", "entities": {"hashtags": [{"start": 83, "end": 113, 
    "tag": "CharlieAndTheChocolateFactory"}], "annotations": [{"start": 0, "end": 10, "probability": 0.9797, "type": "Person", "normalized_text": "Johnny Depp"}, {"start": 84, "end": 112, "probability": 0.8712, "type": "Person", 
    "normalized_text": "CharlieAndTheChocolateFactory"}]}, "lang": "en", "reply_settings": "everyone", "public_metrics": {"retweet_count": 2, "reply_count": 0, "like_count": 0, "quote_count": 0, "impression_count": 0}, 
    "conversation_id": "688089054999633920", "possibly_sensitive": false, "author_id": "1043387532", "edit_history_tweet_ids": ["688089054999633920"], "created_at": "2016-01-15T20:03:09.000Z", "edit_controls": {"edits_remaining": 5, 
    "is_edit_eligible": true, "editable_until": "2016-01-15T20:33:09.000Z"}, "text": "Johnny Depp is not a normal actor. He likes abnormal roles.\rGod. I LOVE LOVE him\ud83d\ude02\ud83d\ude02\r#CharlieAndTheChocolateFactory", 
    "author": {"profile_image_url": "https://pbs.twimg.com/profile_images/1633209667852681220/aUb4JXsD_normal.jpg", "username": "Babygiwa", "id": "1043387532", "pinned_tweet_id": "1119867901283053569", "entities": {"url": {
        "urls": [{"start": 0, "end": 23, "url": "https://t.co/2qVS2yErtW", "expanded_url": "http://glittersushering.com", "display_url": "glittersushering.com"}]}}, "url": "https://t.co/2qVS2yErtW", "verified": false, "name": "Yemi", 
        "description": "Living my best life.", "protected": false, "public_metrics": {"followers_count": 10738, "following_count": 1287, "tweet_count": 353943, "listed_count": 113}, "location": "A place called forward ", 
        "created_at": "2012-12-28T23:37:33.000Z"}}], "created_at": "2022-04-21T09:34:47.000Z", "edit_controls": {"edits_remaining": 5, "is_edit_eligible": true, "editable_until": "2022-04-21T10:04:47.000Z"}, 
        "text": "RT @Babygiwa: Johnny Depp is not a normal actor. He likes abnormal roles.\rGod. I LOVE LOVE him\ud83d\ude02\ud83d\ude02\r#CharlieAndTheChocolateFactory", "author": {
            "profile_image_url": "https://pbs.twimg.com/profile_images/1633209667852681220/aUb4JXsD_normal.jpg", "username": "Babygiwa", "id": "1043387532", "pinned_tweet_id": "1119867901283053569", "entities": {
                "url": {"urls": [{"start": 0, "end": 23, "url": "https://t.co/2qVS2yErtW", "expanded_url": "http://glittersushering.com", "display_url": "glittersushering.com"}]}}, "url": "https://t.co/2qVS2yErtW", 
                "verified": false, "name": "Yemi", "description": "Living my best life.", "protected": false, "public_metrics": {"followers_count": 10738, "following_count": 1287, "tweet_count": 353943, "listed_count": 113}, 
                "location": "A place called forward ", "created_at": "2012-12-28T23:37:33.000Z"}, "__twarc": {"url": "REMOVED", "version": "2.13.0", "retrieved_at": "2023-03-15T15:53:19+00:00"}}"""
    
    yield StringIO(tweet)
    

def test_pandas_read_write_newlines(three_tweets_with_breaks: StringIO):
    df = pd.read_json(three_tweets_with_breaks, lines=True, dtype=False, encoding='utf-8', encoding_errors='backslashreplace', orient='records')
    assert df.shape[0] == 3
    
    csv_buffer = StringIO()
    df.to_csv(csv_buffer,index=False, escapechar='|', encoding='utf-8', errors='backslashreplace', header=False)

    # check we wrote all wrotes to csv
    csv_buffer.seek(0)
    test_output = csv_buffer.readlines()
    assert len(test_output) == 3
    pass

def test_json_surrogate_fails(json_issue_2_minimal: StringIO):
    with pytest.raises(json.JSONDecodeError):
        json.load(json_issue_2_minimal)

def test_json_surrogate_fails_binary(json_issue_2_minimal: BytesIO):
    with pytest.raises(json.JSONDecodeError):
        json.load(json_issue_2_minimal)

def test_pandas_surrogate_fails(json_issue_2_minimal: StringIO):
    for encoding in TEST_ENCODINGS:
        json_issue_2_minimal.seek(0)
        with pytest.raises(UnicodeEncodeError):
            df = pd.read_json(json_issue_2_minimal, encoding=encoding, lines=True, orient='records')
            pass

def test_pandas_surrogate_fails_complete(json_issue_2_complete: StringIO):
    for encoding in TEST_ENCODINGS:
        json_issue_2_complete.seek(0)
        with pytest.raises((UnicodeEncodeError, AssertionError)):
            df = pd.read_json(json_issue_2_complete, encoding=encoding, lines=True, orient='records')
            pass

def test_pandas_read_binary(json_issue_2_minimal_binary: StringIO):
    df = pd.read_json(json_issue_2_minimal, encoding='utf-8')

def test_pandas_to_csv_surrogate_fails(json_issue_2_minimal: StringIO):
    with pytest.raises(UnicodeEncodeError):
        df = df.to_csv(json_issue_2_minimal,index=False, escapechar='|', encoding='utf-8', errors='backslashreplace')

def test_pandas_utf16(json_issue_2_minimal: StringIO):    
    test_input = BytesIO(json_issue_2_minimal.encode('utf-8', errors='backslashreplace'))
    df = pd.read_json(test_input, lines=True, dtype=False, encoding='utf-8', encoding_errors='backslashreplace', orient='records')
    
    # check we read all input and only produced three rows
    assert len(df['text'].values) == 3
    assert df.shape[0] == 3

    csv_buffer = StringIO()
    df.to_csv(csv_buffer,index=False, escapechar='|', encoding='utf-8', errors='backslashreplace')

    # check we wrote all wrotes to csv
    csv_buffer.seek(0)
    test_output = csv_buffer.readlines()
    assert len(test_output) == 3
    pass


def test_utf16_retweets(json_issue_2_complete):
    from DATA_collector.src.config import Schematype
    from DATA_collector.src.data import process_json_data
    tweet_count, list_of_dataframes = process_json_data(json_issue_2_complete, csv_filepath=None, bq=None, project=None, dataset=None, subquery=None, start_date=None, end_date=None, archive_search_counts=0, tweet_count=0, schematype=Schematype.DATA, test=True)

    tweets = list_of_dataframes[0]
    csv_buffer = BytesIO()
    tweet_rows = tweets.to_csv(csv_buffer,index=False, escapechar='|')
    
    # We expect only one row:
    assert len(tweet_rows) == 1


def test_utf16_retweets_surrogates(json_issue_2_complete):
    from DATA_collector.src.config import Schematype
    from DATA_collector.src.data import process_json_data
    tweet_count, list_of_dataframes = process_json_data(json_issue_2_complete, csv_filepath=None, bq=None, project=None, dataset=None, subquery=None, start_date=None, end_date=None, archive_search_counts=0, tweet_count=0, schematype=Schematype.DATA, test=True)

    tweets = list_of_dataframes[0]
    csv_buffer = BytesIO()
    tweet_rows = tweets.to_csv(csv_buffer,index=False, escapechar='|', encoding='utf-8', errors='ignore')
    
    # We expect only one row:
    assert len(tweet_rows) == 1
