# Frequently Asked Questions

<br>
<br>

#### I want to search for an exact string. How do I format this in config.yml?
You need to use double quotation marks to search exact strings. For example, if your query is 'cats and dogs', you will need to format this as `'"cats and dogs"'` in config.yml

<br>
<br>

#### I have multiple queries that I would like to collect consecutively. Can I do that?
Yes - just ensure that your strings are separated by commas and enclosed within square brackets, like so: 
`['cats OR dogs', 'kittens and puppies', 'chickens and chicks']`
DATA will run a separate search for each query, and will upload all data to the dataset specified in config.py

<br>
<br>

#### Can I use DATA to search for Tweets without uploading to BigQuery?
No - if you do not want any data processing done, [Twarc](https://twarc-project.readthedocs.io/en/latest/twarc2_en_us/) is your best option. This tool uses the Twarc library to collect Twitter data, but is designed to process these files and store them in Google BigQuery.

<br>
<br>

#### Why is there no command line option for config?
This tool was designed specifically for researchers who requested a largely code-free option for streamlining the gathering, processing and storage of data from Twitter. For this reason, the config is set outside of the command line.

<br>
<br>

#### I am running out of space on my computer - can I move collected files?
Yes, you can move all but the newest file in your collection directory to a backup location. If you have to stop and restart DATA, and you have moved some files from your collection, make sure to update the start date in config.yml, so that you don't accidentally re-collect the data you have backed up.

<br>
<br>

#### Is there anything I can't search for using DATA?
You can search for anything, as long as it meets Twitter's rules for [How to Build a Query](https://developer.twitter.com/en/docs/twitter-api/tweets/search/integrate/build-a-query#build).

<br>
<br>

#### My counts are slightly higher than my actual retrieved Tweets. Why is this?
According to Twitter, the counts endpoint is not subject to the same compliance checks as the search endpoint. For this reason, your counts are likely to be a bit higher than what you actually gather.

<br>
<br>