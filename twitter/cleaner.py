import unicodedata


def normalize_tweet(tweet):
    tweet = unicodedata.normalize('NFKD', tweet).encode('ascii', 'ignore').decode('utf-8', 'ignore')
    return tweet


def remove_urls(tweet):
    return ' '.join([word for word in tweet.split() if not word.startswith('http')])


def remove_mentions(tweet):
    return ' '.join([word for word in tweet.split() if not word.startswith('@')])


def remove_hashtags(tweet):
    return ' '.join([word for word in tweet.split() if not word.startswith('#')])


def remove_punctuation(tweet):
    return ''.join([char for char in tweet if char not in '!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~'])


class Cleaner(object):
    def __init__(self, data):
        self.data = data
        with open('stopwords.txt', 'r') as f:
            self.stopwords = f.read().splitlines()

    def remove_stopwords(self, tweet):
        return ' '.join([word for word in tweet.split() if word not in self.stopwords])

    def clean(self):
        data = self.partial_clean()
        for tweet in data:
            tweet.text = tweet.data['text'] = self.remove_stopwords(tweet.text)
        return data

    def partial_clean(self):
        for tweet in self.data:
            tweet.text = tweet.data['text'] = tweet.text.strip()
            tweet.text = tweet.data['text'] = normalize_tweet(tweet.text)
            tweet.text = tweet.data['text'] = remove_urls(tweet.text)
            tweet.text = tweet.data['text'] = remove_mentions(tweet.text)
            tweet.text = tweet.data['text'] = remove_hashtags(tweet.text)
            tweet.text = tweet.data['text'] = remove_punctuation(tweet.text)
        return self.data
