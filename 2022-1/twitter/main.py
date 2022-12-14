import os

from scrapper import TwitterClient
from writer import Writer
from datetime import datetime
from cleaner import Cleaner
from analyzer import Analyzer

if __name__ == '__main__':
    twitter_client = TwitterClient(bearer_token=os.getenv('TWITTER_BEARER_TOKEN'))
    tweets_search = twitter_client.get_recent_tweets_by_subject_with_pagination(subject="enem",
                                                                                query_params="-is:retweet -is:quote -is:reply",
                                                                                max_results=9000)
    all_tweets = []
    for tweet in tweets_search:
        all_tweets.append(tweet)

    writer_tweets_json = Writer(file_name=f'tweets-{datetime.now().strftime("%Y-%m-%d")}.json',
                                output_dir=f"./output/tweets")

    writer_tweets_json.write([tweet.data for tweet in all_tweets])
    cleaner = Cleaner(all_tweets)
    tweets_parcial_clean = cleaner.partial_clean()

    analyzer = Analyzer(tweets_parcial_clean)
    analyzer.generate_sentiment_by_tweet()
    analyzer.populate_state_info()
    analyzer.populate_coords()
    analyzer.save_df()

    tweets_clean = cleaner.clean()
    writer_tweets_clean_json = Writer(file_name=f'tweets-{datetime.now().strftime("%Y-%m-%d")}.json',
                                      output_dir=f"./output/tweets/clean")

    writer_tweets_clean_json.write([tweet.data for tweet in tweets_clean])
