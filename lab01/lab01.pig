-- DS410 Lab 1: Pig Example
-- Task: Compute the total number of retweets for each tweeter.

data = LOAD 'hdfs:/ds410/lab1/tweets.csv' using PigStorage(',') AS (tweeter: chararray, text: chararray, retweet: int);
tweeters = GROUP data BY tweeter;
counts = FOREACH tweeters GENERATE group, SUM(data.retweet);

DESCRIBE counts;
DUMP counts;
