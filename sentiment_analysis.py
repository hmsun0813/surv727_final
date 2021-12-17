import pandas as pd
import numpy as np
from textblob import TextBlob
from nltk import tokenize
from nltk.sentiment.vader import SentimentIntensityAnalyzer


def get_subjectivity(text):
    """
    Parameters:
        text (str): source data
        
    Return:
        subjectivity_score (float)
    """

    return TextBlob(text).sentiment.subjectivity


def get_vader_score(text):
    """
    Caluculate the average vader assessment scores for the input text
    
    Parameters:
        text (str): source data
        
    Return:
        vader_scores (dict): a dict contains compound score, negitive score,
            neutral score, positive score.
    """

    # Initialize settings
    sentences = tokenize.sent_tokenize(text)
    vader_scores = {
        'compound': [],
        'neg': [],
        'neu': [],
        'pos': [],
    }
    sid = SentimentIntensityAnalyzer()

    # Calculate Vader scores for each individual sentence
    for sentence in sentences:
        polarity = sid.polarity_scores(sentence)
        for key in polarity.keys():
            vader_scores[key].append(polarity[key])

    # Calculate the average Vader scores for the input text
    for key, val in vader_scores.items():
        vader_scores[key] = np.nanmean(val)

    return vader_scores


def main():

    tweets = pd.read_csv("text_processed.csv")

    tweets['subjectivity'] = tweets['text_cleaned'].apply(get_subjectivity)
    tweets['polarity'] = tweets['text_cleaned'].apply(get_vader_score)
    tweet_sentiment = pd.concat([tweets, tweets['polarity'].apply(pd.Series)], axis=1)
    tweet_sentiment = tweets.drop(['polarity', 'Unnamed: 0'], axis=1)

    tweet_sentiment.to_csv('tweet_sentiment.csv')


if __name__ == "__main__":
    main()