import tweepy


class TwitterClient(object):
    tweet_fields = ['id', 'text', 'created_at', 'geo', 'source']
    place_fields = ['place_type', 'geo', 'name']
    expanions = ['author_id', 'geo.place_id']
    user_fields = ['location']

    def __init__(self, bearer_token):
        self.api = tweepy.Client(bearer_token=bearer_token)

    def get_recent_tweets_by_subject(self, subject, count=10, query_params=None):
        tweets = self.api.search_recent_tweets(query=f"{subject} {query_params}", max_results=count,
                                               tweet_fields=self.tweet_fields, place_fields=self.place_fields,
                                               user_fields=self.user_fields, expansions=self.expanions)
        return tweets.data

    def get_recent_tweets_by_subject_with_pagination(self, subject, query_params=None, max_results=10):
        tweets = tweepy.Paginator(self.api.search_recent_tweets, query=f"{subject} {query_params}", max_results=100,
                                  tweet_fields=self.tweet_fields,
                                  place_fields=self.place_fields,
                                  user_fields=self.user_fields,
                                  expansions=self.expanions).flatten(limit=max_results)
        return tweets

    def get_tweet_by_id(self, tweet_id):
        tweet = self.api.get_tweet(tweet_id, tweet_fields=self.tweet_fields, place_fields=self.place_fields,
                                   user_fields=self.user_fields, expansions=self.expanions)
        return tweet.data
