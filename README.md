# DMRC Academic Twitter Archive Collector


### Overview

------------------------------------
The DMRC Academic Twitter Archive (DATA) Collector uses the Twarc Python library to pull tweets from Twitter's archive, via the API 2.0. It then processes the resulting json files and pushes the data to a designated Google BigQuery database.
Tweets can be collected as far back as 22 Mar, 2006. 

DATA can be used to append data to an existing TCAT or TweetQuery dataset.
You can now also upload a previously collected Twitter API 2.0 json file (e.g. from Twitter's Tweet Downloader) to be processed and pushed to Google BigQuery. *** This is still being tested.

<br>


### Who is this for?

------------------------------------
This tool is intended for researchers at the Digital Media Research Centre who wish to collect data from the Twitter archive (more than one week in the past), and have these data pushed automatically to Google BigQuery.

<br>


### What You Will Need

------------------------------------
1. Python 3.8 or newer
2. A valid [Twitter Academic API bearer token](https://developer.twitter.com/en/products/twitter-api/academic-research)
3. A valid [Google service account and json key](https://cloud.google.com/iam/docs/creating-managing-service-account-keys)
4. `xGB` free on your local drive for json file storage (the following are estimates and may differ depending on the data collected; you can store these files elsewhere after collection):
      
| n Tweets   | Size (GB) |
|------------|-----------|
| 250,000    | ~ 1.25    |
| 500,000    | 2 - 2.5   |
| 1,000,000  | 4 - 5     |
| 5,000,000  | 16 - 18   |
| 10,000,000 | 30 - 35   |
<br>

### To Use

------------------------------------

#### If you HAVEN'T already cloned this repository:
<br>  

#### `In cmd/terminal:`
1. Clone this repository to a location with enough space to download tweets (refer to <b>What You Will Need</b> section, above): 
`git clone https://github.com/qut-dmrc/DMRC_Academic_Twitter_Archive_Collector.git`.
####
2. Navigate to the cloned  directory: `cd DMRC_Academic_Twitter_Archive_Collector`.
####
3. Install venv requirements: `python -m pip install -r requirements.txt`.
####
4. Navigate to the collector: `cd DATA_collector`.
####
<br>  

#### `In your file explorer:`
5. Place your Google BigQuery service key json file into the `DATA_collector/access_key` directory.
####
6. Open `DATA_collector/config/config_template.yml`.
      1. Set your query parameters:
         * <b>query:</b> string containing keyword(s) and/or phrase(s), e.g. 'winter OR cold' 
         * <b>query_list</b>: a list of queries, in testing - leave as is.
         * <b>start_date:</b> the earliest date to search, in UTC time.
         * <b>end_date:</b> the latest date to search, in UTC time.
      ####
      2. Enter your bearer token:
         * <b>bearer_token</b>: your Twitter Academic API bearer token.
      ####
      3. Set your Google BigQuery project and dataset:
         * <b>project_id:</b> name of the relevant Google BigQuery billing project, e.g. 'dmrc-data'.
         * <b>dataset:</b> the name of your intended dataset, e.g. 'winter2022'. If it already exists, the data will be appended to the existing dataset; if it does not exist, a new dataset will be created.
      ####
      4. Enter your email address:
         * <b>user_email:</b> your email address, to notify you by email of the search's completion.
      ####
      5. Choose your <b>schema type</b> (DATA, TCAT, TweetQuery). `DATA = True` by default. Refer to <b>Output</b>, below, for schema details.
####
7. Rename `config_template.yml` to `config.yml`.
####
<br>  

#### `In cmd/terminal:` 
8. Run `python ./run.py`.
###
####
After you run `run.py`, you will be prompted to verify your query config details. If everything is correct, type `y`, otherwise, type `n` to exit and change your input.
<br>
<br>
<br>
#### !!! If you HAVE already cloned this repository:
There is a very good chance that (beneficial!) changes have been made to this repository. Remember to update before you use DATA using 
`git pull origin main`!
<br>
<br>
<br>
### Output

------------------------------------
Depending on the schema type selected, the tool will produce data as shown below:

| Schema Type | Purpose                                                                                                | n Tables | Table Names                                                                                                                                                                                                              |
|-------------|--------------------------------------------------------------------------------------------------------|----------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| DATA        | Standalone archive data analysis, where it is not necessary to append archive data to existing tables. | 12       | annotations<br/>author_description<br/>author_urls<br/>context_annotations<br/>hashtags<br/>interactions<br/>media<br/>mentions<br/>poll_options<br/>tweets<br/>urls<br>edit_history (for tweets later than August 2022) |
| TCAT        | Backfill/append archive data to an existing TCAT table                                                 | 3        | hashtags<br/>mentions<br/>tweets                                                                                                                                                                                         |
| TweetQuery  | Backfill/append archive data to an existing TweetQuery table                                           | 1        | tweets_flat                                                                                                                                                                                                              |


A detailed overview of the tables and fields can be located here (TBC - provide link)
<br>

On successful completion of an archive search, you will receive an email from the DMRC Academic Twitter App, containing details of your search, including your search query, location of your data, the number of tweets collected and the time taken to perform the search.
<br>
<br>
<br>

### How to Build a Query

------------------------------------
####
#### Query string
Your query string should follow Twitter's rules for [How to Build a Query](https://developer.twitter.com/en/docs/twitter-api/tweets/search/integrate/build-a-query#build).
####
Queries may be up to 1024 characters long.
####
Queries are case insensitive.

<br>


#### Operator logic

| Operator    | Logic    | Example                           | What it does                                                                            |
|-------------|----------|-----------------------------------|-----------------------------------------------------------------------------------------|
|   | AND      | frosty snowman                    | searches for tweets that contain keywords 'frosty' AND 'snowman'                        |
| OR          | OR       | frosty OR snowman                 | searches for tweets that contain keywords 'frosty' OR 'snowman'                         |
| -           | NOT      | frosty -snowman                   | searches for tweets that contain keywords 'frosty', but NOT 'snowman'                   |
| (&nbsp;&nbsp;) | Grouping | (frosty OR snowman) (carrot nose) | searches for tweets that contain keywords 'frosty' or 'snowman' AND 'carrot' AND 'nose' |  
| " "       | Exact string | "frosty the snowman" | searches for the exact string as a keyword, e.g. "frosty the snowman" OR carrot |

<br>

#### Order of operations
AND operators are evaluated before OR operators. For example, 'frosty OR snowman carrot' will be evaluated as 'frosty OR (snowman AND carrot)'.
####







<br>
<br>
