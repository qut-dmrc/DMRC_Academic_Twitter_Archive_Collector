import csv
from io import StringIO, BytesIO
import pytest
import pandas as pd
import json


TEST_ENCODINGS = ['utf-8', 'utf-16', 'utf-32']
ISSUE_2_JSON = '../tests/data/issue_002a.json'
ISSUE_2_CSV = '../tests/data/issue_2.csv'
ISSUE_2_CORRECT_TEXT = "LOVE LOVE him😂😂"
ISSUE_2_JSON_B = '../tests/data/issue_002b.json'

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
        {"text": "normal tweet\r\n without\n\r \n \rhashtag"}
    """
    yield StringIO(tweet)

@pytest.fixture
def json_issue_2_minimal_binary():
    tweet = b"""
                {"text": "normal \r\ntweet\r with\n \r #hashtag"}
                {"text": "roles.\rtext\ud83d\ude02\ud83d\ude02\r#Chashtag"}
                {"text": "normal LOVE LOVE him\ud83d\ude02\ud83d\ude02\r#CharlieAn\rhashtag"}
                {"text": "normal tweet\r\n without\n\r \n \rhashtag"}
                """
    yield BytesIO(tweet)

@pytest.fixture
def json_issue_2_complete():
    with open(ISSUE_2_JSON, 'r') as f:
        yield f
    
@pytest.fixture
def json_issue_2_complete_binary():
    with open(ISSUE_2_JSON, 'rb') as f:
        yield f

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
            assert df['text'].str.contains(ISSUE_2_CORRECT_TEXT).any()

def test_pandas_reads_json_file():
    encoding = 'utf-8'
    df = pd.read_json(ISSUE_2_JSON, encoding=encoding, lines=True, orient='records')
    assert df.shape[0] == 1
    assert ISSUE_2_CORRECT_TEXT in df.loc[0, 'text']

def test_pandas_read_binary(json_issue_2_minimal_binary: BytesIO):
    encoding = 'utf-8'
    df = pd.read_json(json_issue_2_minimal_binary, encoding=encoding, lines=True, orient='records')
    assert df.shape[0] == 4
    assert df['text'].str.contains(ISSUE_2_CORRECT_TEXT).any()

def test_pandas_to_csv_surrogate(json_issue_2_minimal_binary: StringIO):
    encoding = 'utf-8'
    df = pd.read_json(json_issue_2_minimal_binary, encoding=encoding, lines=True, orient='records')
    
    csv_buffer = StringIO()
    df.to_csv(csv_buffer,index=False, escapechar='|', encoding='utf-8')
    csv_buffer.seek(0)
    output = '\n'.join(csv_buffer.readlines())
    assert ISSUE_2_CORRECT_TEXT in output

def test_pandas_read_write():    
    df = pd.read_json(ISSUE_2_JSON, lines=True, dtype=False, encoding='utf-8', orient='records')
    
    # check we read all input
    assert len(df['text'].values) == 1
    assert ISSUE_2_CORRECT_TEXT in df.loc[0, 'text']
    
    df.to_csv(ISSUE_2_CSV,index=False, escapechar='|', encoding='utf-8', header=False)

    # check we wrote all wrotes to csv -- and no more
    with open(ISSUE_2_CSV) as f:
        output = f.readlines()
        
    assert len(output) == 1  # no header
    assert ISSUE_2_CORRECT_TEXT in output[0]
    
def test_pandas_read_write_b():    
    df = pd.read_json(ISSUE_2_JSON_B, lines=True, dtype=False, encoding='utf-8', orient='records')
    
    # check we read all input
    assert len(df['text'].values) == 1
    
    df.to_csv(ISSUE_2_CSV,index=False, escapechar='|', encoding='utf-8', header=False, quoting=csv.QUOTE_ALL)

    # check we wrote all wrotes to csv -- and no more
    with open(ISSUE_2_CSV) as f:
        output = f.readlines()
        
    assert len(output) == 1  # no header

def test_retweets_integrated():
    from DATA_collector.src.config import Schematype
    from DATA_collector.src.data import process_json_data, write_processed_data_to_csv
    
    tweet_count, list_of_dataframes = process_json_data(ISSUE_2_JSON, csv_filepath=None, bq=None, project=None, dataset=None, subquery=None, start_date=None, end_date=None, archive_search_counts=0, tweet_count=0, schematype=Schematype.DATA, test=True)

    tweets = list_of_dataframes[0]
    write_processed_data_to_csv(tweets, csv_file='', csv_filepath=ISSUE_2_CSV)
    
    with open(ISSUE_2_CSV) as f:
        output = f.readlines()
        
    # We expect only one row:
    assert len(output) == 2  # includes header
    assert ISSUE_2_CORRECT_TEXT in output


def test_newlines_integrated():
    from DATA_collector.src.config import Schematype
    from DATA_collector.src.data import process_json_data, write_processed_data_to_csv
    
    tweet_count, list_of_dataframes = process_json_data(ISSUE_2_JSON_B, csv_filepath=None, bq=None, project=None, dataset=None, subquery=None, start_date=None, end_date=None, archive_search_counts=0, tweet_count=0, schematype=Schematype.DATA, test=True)

    tweets = list_of_dataframes[0]
    write_processed_data_to_csv(tweets, csv_file='', csv_filepath=ISSUE_2_CSV)
    
    with open(ISSUE_2_CSV) as f:
        output = f.readlines()
        
    # We expect only one row:
    assert len(output) == 2  # includes header

