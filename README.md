# DMRC Academic Twitter Archive Collector

This program uses the Twarc Python library to pull tweets from Twitter's archive, via the API 2.0. It then processes the resulting json files and pushes the data to a designated Google BigQuery database.
<br>
<br>
<br>

### Who is this for?
This program is intended for researchers at the Digital Media Research Centre who wish to collect Twitter data from the archive (more than one week in the past).
<br>
<br>

### You Will Need
1. Python 3.9 or later
2. A valid Twitter Academic API bearer token
3. A valid Google service account and json key
<br>

### To Use
1. Clone this repository.
####
2. Install venv requirements from <b>requirements.txt</b>. 
####
3. Paste your Google BigQuery service key json file into the <b>DATA_collector/access_key</b> directory, or set your Google environment variables.
####
4. Open <b>DATA_collector/config/config_template.py</b>.
      1. Set your query parameters:
         * <b>query:</b> e.g. 'netflix OR stan'
         * <b>start_date:</b> the earliest date to search, in UTC time.
         * <b>end_date:</b> the latest date to search, in UTC time.
         * <b>interval_days:</b> the number of days covered per json file; default 1
      ####
      2. Add your bearer token:
         * <b>bearer_token</b>: your Twitter Academic API bearer token.
      ####
      3. Set your Google BigQuery project and dataset:
         * <b>project_id:</b> name of the relevant Google BigQuery billing project.
         * <b>dataset:</b> the name of your intended dataset. If it exists, the data will be appended to the existing dataset; if it does not exist, a new dataset will be created.
      ####
      4. Add your email address:
         * <b>user_email:</b> your email address, if you would like to receive an email to nofity you of the search's completion.
      ####
      5. Choose your <b>schema type</b> (DATA, TCAT, TweetQuery), e.g. DATA = True
####
5. Rename <b>config_template.py</b> to <b>config.py</b>
####
6. Run <b>run.py</b>.
<br>
<br>

### Output
This tool produces 11 tables:
   * annotations
   * author_description
   * author_urls
   * context_annotations
   * hashtags
   * interactions
   * media
   * mentions
   * poll_options
   * tweets
   * urls

A detailed overview of the tables and fields can be located here (TBC)
<br>
<br>

### How to Build a Query
TBC
<br>
<br>
