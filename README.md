# DMRC Academic Twitter Archive Collector

The DMRC Academic Twitter Archive Collector uses the Twarc Python library to pull tweets from Twitter's archive, via the API 2.0. It then processes the resulting json files and pushes the data to a designated Google BigQuery database.
<br>
<br>
<br>


### Overview

------------------------------------
Overview here... To be completed

<br>


### Who is this for?

------------------------------------
This tool is intended for researchers at the Digital Media Research Centre who wish to collect data from the Twitter archive (more than one week in the past), and have these data pushed automatically to Google BigQuery.

<br>


### What You Will Need

------------------------------------
1. Python 3.10 or later
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
1. Clone this repository to a location with enough space to download tweets (refer to <b>What You Will Need</b> section, above).
####
2. Install venv requirements:`pip install -r requirements.txt`
####
3. Place your Google BigQuery service key json file into the `DATA_collector/access_key` directory.
####
4. Open `DATA_collector/config/config_template.yml`.
      1. Set your query parameters:
         * <b>query:</b> string containing keyword(s) and/or phrase(s), e.g. 'winter OR cold' 
         * <b>start_date:</b> the earliest date to search, in UTC time.
         * <b>end_date:</b> the latest date to search, in UTC time.
         * <b>interval_days:</b> the number of days covered per json file collected; default 1*
           * Increasing the interval to 7 = one week at a time, 30 = one month at a time, etc. Increasing the interval can speed up collection where volume of tweets is low relative to the duration of the search.
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
      5. Choose your <b>schema type</b> (DATA, TCAT, TweetQuery). DATA = True by default. Refer to <b>Output</b>, below, for schema details.
####
5. Rename `config_template.yml` to `config.yml`.
####
6. In your terminal, navigate to `cd .\DATA_collector\` and run `python ./run.py`.
####
After you run `run.py`, you will be prompted to verify your query config details. If everything is correct, type 'y', otherwise, type 'n' to exit and change your input.
<br>
<br>
<br>
### Output

------------------------------------
Depending on the schema type selected, the tool will produce data as shown below:

| Schema Type | Purpose                                                                                                | n Tables | Table Names                                                                                                                                                          |
|-------------|--------------------------------------------------------------------------------------------------------|----------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| DATA        | Standalone archive data analysis, where it is not necessary to append archive data to existing tables. | 11       | annotations<br/>author_description<br/>author_urls<br/>context_annotations<br/>hashtags<br/>interactions<br/>media<br/>mentions<br/>poll_options<br/>tweets<br/>urls |
| TCAT        | Backfill/append archive data to an existing TCAT table                                                 | 3        | hashtags<br/>mentions<br/>tweets                                                                                                                                     |
| TweetQuery  | Backfill/append archive data to an existing TweetQuery table                                           | 3        | hashtags<br/>mentions<br/>tweets                                                                                                                                     |


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

| Operator        | Logic    | Example                           | What it does                                                                            |
|-----------------|----------|-----------------------------------|-----------------------------------------------------------------------------------------|
| '&nbsp;&nbsp;'  | AND      | frosty snowman                    | searches for tweets that contain keywords 'frosty' AND 'snowman'                        |
| 'OR'            | OR       | frosty OR snowman                 | searches for tweets that contain keywords 'frosty' OR 'snowman'                         |
| '-'             | NOT      | frosty -snowman                   | searches for tweets that contain keywords 'frosty', but NOT 'snowman'                   |
| (&nbsp;&nbsp;)  | Grouping | (frosty OR snowman) (carrot nose) | searches for tweets that contain keywords 'frosty' or 'snowman' AND 'carrot' AND 'nose' |                                                       

<br>

#### Order of operations
AND operators are evaluated before OR operators. For example, 'frosty OR snowman carrot' will be evaluated as 'frosty OR (snowman AND carrot)'.
####







<br>
<br>
