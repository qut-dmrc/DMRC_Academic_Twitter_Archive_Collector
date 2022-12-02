# DMRC Academic Twitter Archive Collector

<br>
<br>
<br>

### Overview

------------------------------------
The <b>DMRC Academic Twitter Archive (DATA)</b> Collector uses the Twarc Python library to collect tweets from Twitter's archive, via the API 2.0 for academic research. It then processes the collected .json files and pushes the data to a designated Google BigQuery database.
Tweets can be collected from as far back as 22 Mar, 2006. 

This tool can also be used to append archive data to an existing <b>TCAT</b> or <b>TweetQuery</b> dataset, and can serve as a backfill tool to supplement previously-collected datasets.


You can now also upload a previously collected Twitter API 2.0 json file (e.g. from Twitter's Tweet Downloader) to be processed and pushed to Google BigQuery * . 

<br>
<br>
<br>

### Who is this for?

------------------------------------
This tool is intended for researchers who wish to collect data from the Twitter archive (more than one week in the past), and have these data processed and pushed automatically to Google BigQuery. It was designed with DMRC researchers in mind, but may be useful to researchers from other institutions.

<br>
<br>
<br>

### What You Will Need

------------------------------------
1. Python 3.8 or newer
2. A valid [Twitter Academic API bearer token](https://developer.twitter.com/en/products/twitter-api/academic-research)
3. A valid [Google service account and json key](https://cloud.google.com/iam/docs/creating-managing-service-account-keys)
4. `xGB` free on your local drive for json file storage (the following are estimates and may differ depending on the data collected; you can store these files elsewhere after collection; see <b>Managing Disk Space</b>, below):
      
| n Tweets   | Size (GB) |
|------------|-----------|
| 250,000    | ~ 1.25    |
| 500,000    | 2 - 2.5   |
| 1,000,000  | 4 - 5     |
| 5,000,000  | 16 - 18   |
| 10,000,000 | 30 - 35   |

<br>
<br>
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
6. Navigate to the DATA_collector directory (e.g. `C:/Users/You/Desktop/DMRC_Academic_Twitter_Archive_Collector/DATA_collector`).
####
7. Place your Google BigQuery service key json file into the `DATA_collector/access_key` directory.
####
8. Open `DATA_collector/config/config_template.yml`.
      1. Set your query parameters:
         * <b>query:</b> string containing keyword(s) and/or phrase(s), e.g. 'winter OR cold'
         * <b>start_date:</b> the earliest date to search, in UTC time.
         * <b>end_date:</b> the latest date to search, in UTC time.
      ####
      2. Enter your bearer token:
         * <b>bearer_token</b>: your Twitter Academic API bearer token.
      ####
      3. Set your Google BigQuery project and dataset:
         * <b>project_id:</b> name of the relevant Google BigQuery billing project. Must match the provided service account key.
         * <b>dataset:</b> the name of your intended dataset, e.g. 'winter2022'. If it already exists, the data will be appended to the existing dataset; if it does not exist, a new dataset will be created.
      ####
      4. Choose your <b>schema type</b> (DATA, TCAT, TweetQuery). `DATA = True` by default. Refer to <b>Output</b>, below, for schema details.
####
9. Rename `config_template.yml` to `config.yml`.
####
<br>  

#### `In cmd/terminal:` 
10. Run `python ./run.py`.
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


A detailed overview of the tables and fields is located at `DMRC_Academic_Twitter_Archive_Collector/supp_docs/schema_descriptions.txt`
<br>

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
<br>


### Tweet reference levels

------------------------------
The DATA tool collects Twitter data using the <b>Twarc2</b> library. During processing, it generates several extra fields to enrich these data. One such field is called <b>'reference_level'</b>.

Tweets that match the search parameters are situated at reference_level 0. 
If any tweets at reference level 0 happen to reference another tweet in any way (i.e. retweet, quote, reply), then that referenced tweet will be situated at reference_level 1, and so on. 
Unless you are interested in these referenced tweets, it can be useful to filter the data to include Tweets where reference_level=0 at the time of analysis. This focuses your analysis on tweets that directly match your search parameters, 
and reduces the size of your working dataset.

<br>
<br>
<br>


### Uploading a .json file to be processed

------------------------------------
If you have a .json file from Twitter's Tweet Downloader, you can have this processed and pushed to a Google BigQuery dataset by following these steps:

`TBC` '* Load from 'Tweet Collector' json currently in testing phase.

<br>
<br>
<br>

### Managing disk space

------------------------------------
####
This tool is designed to run on a user's local computer. In order to keep collected file sizes manageable, collections are split into files containing an average 150,000 tweets. This means that for collections greater than this, files will average approximately 1GB in size.
If you need to clear some space, you can remove collected .json files from the `DATA_Collector/your_directory/collected_json folder` while the collector is running, and move them to a backup location.

If you do remove files from this location, and need to stop/restart the collector, <b>you will need to update the start_date parameter in `config.yml` to avoid re-collecting those files</b>. 
Be sure not to remove the current collection file (the newest file in the directory).
####
<br>
<br>
<br>

### Logging

------------------------------------
####
A timestamped log file will be generated at location `DATA_collector/logging` each time you run a collection.
This log file contains all outputs and can aid in investigating any errors that may arise.
<br>
<br>
<br>

