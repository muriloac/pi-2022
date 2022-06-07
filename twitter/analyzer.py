import pandas as pd
import numpy as np
import os
import re
from textblob import TextBlob
from textblob.classifiers import NaiveBayesClassifier
from datetime import datetime
from geopy.geocoders import Nominatim
import unicodedata
import time

app_geo = Nominatim(user_agent="twitter_scrapper_analyzer")


def create_dataframe(data):
    tweets_df = pd.DataFrame(data, columns=['Tweets'])

    tweets_df['Tweets'] = np.array([tweet.text for tweet in data])
    tweets_df['len'] = np.array([len(tweet.text) for tweet in data])
    tweets_df['ID'] = np.array([tweet.id for tweet in data])
    tweets_df['Date'] = np.array([tweet.created_at for tweet in data])
    tweets_df['Geo'] = np.array([tweet.geo for tweet in data])
    tweets_df['Source'] = np.array([tweet.source for tweet in data])
    return tweets_df


def generate_train_model():
    palavras = './ReLi-Lex/'
    train = []
    palavras_pt_br = []
    palavras_sentimentos_pt_br = []

    files = [os.path.join(palavras, f) for f in os.listdir(palavras)]

    for file in files:
        t = 1 if '_Positivos' in file else -1
        with open(file, 'r') as arquivo:
            conteudo = arquivo.read()
            tudo = re.findall('\[.*?\]', conteudo)
            for p in tudo:
                aux = unicodedata.normalize('NFKD', (p[1:-1])).encode('ascii', 'ignore').decode('utf-8', 'ignore')
                palavras_pt_br.append(aux)
                palavras_sentimentos_pt_br.append(t)
                train.append((aux, t))
    return train


def get_address_by_location(latitude, longitude, language="en"):
    coordinates = f"{latitude}, {longitude}"
    time.sleep(1)
    return app_geo.reverse(coordinates, language=language).raw


def get_state_info(geo):
    try:
        if geo is not None:
            lat = geo['coordinates']['coordinates'][1]
            lon = geo['coordinates']['coordinates'][0]
            return get_address_by_location(lat, lon)['address']['state']
    except Exception:
        return None


def get_coordinates_info(tweet):
    try:
        if tweet is not None:
            lat = tweet['coordinates']['coordinates'][1]
            lon = tweet['coordinates']['coordinates'][0]
            return f"{lat}, {lon}"
    except Exception:
        return None


class Analyzer(object):

    def __init__(self, data):
        self.data = data
        self.df = create_dataframe(data)
        self.cl = NaiveBayesClassifier(generate_train_model())

    def return_df(self):
        print(self.df.head())

    def save_df(self):
        self.df.to_csv(f'./output/tweets_analysis/tweets-analisados-{datetime.now().strftime("%Y-%m-%d")}.csv',
                       index=False)

    def generate_sentiment(self, tweet):
        polarity = 0
        blob = TextBlob(tweet, classifier=self.cl)

        for s in blob.sentences:
            polarity = s.classify() + polarity
        # POSITIVO
        if polarity > 0:
            return 1
        # NEGATIVO
        elif polarity < 0:
            return -1
        # NEUTRO
        else:
            return 0

    def generate_sentiment_by_tweet(self):
        self.df['SA'] = np.array([self.generate_sentiment(tweet) for tweet in self.df['Tweets']])

    def populate_state_info(self):
        self.df['Estado'] = np.array([get_state_info(tweet) for tweet in self.df['Geo']])

    def populate_coords(self):
        self.df['Coordenadas'] = np.array([get_coordinates_info(tweet) for tweet in self.df['Geo']])
