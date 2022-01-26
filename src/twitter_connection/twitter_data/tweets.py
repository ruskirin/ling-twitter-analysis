import re
import logging
import pandas as pd
import emoji

from unidecode import unidecode
from twitter_data import twitter_data


class Tweets(twitter_data.TwitterData):
    def __init__(self, data=None):
        super().__init__(data)

        if data is not None:
            # Change column names
            self.rename(twitter_data.conf['rename_maps']['tweets'])
            # Append a column with normalized text
            self.data = pd.concat([self.data, self.normalize(data)], axis=1)

    def normalize(self, data):
        return data.loc[:, 'text_orig']\
            .apply(self.norm_text)\
            .apply(unidecode)\
            .rename('text_norm')

    # Must precede unidecode otherwise text formatting might cause issues
    @staticmethod
    def norm_text(tweet):
        t = re.sub(r'@[\w]+[\b ]*', '', tweet)
        t = re.sub(r'[\t]+|[\n]+', ' ', t)
        # Props to: https://stackoverflow.com/a/50602709/13557629
        return emoji.get_emoji_regexp('es').sub(r'', t)

    def append(self, other):
        try:
            super().append(other)

            if self.data is None:
                # If no data stored then initialize
                self.data = other.data

                self.rename(twitter_data.conf['rename_maps']['tweets'])

            else:
                self.data = self.data.append(
                    other.data, ignore_index=True)
        except Exception as e:
            logging.exception(e.args)
            print(f'Failed to append data!')

    def save_csv(self, path, lang, topic, num):
        self.remove_dups('tweet_id')
        super().save_csv(path, lang, topic, num)

    @classmethod
    def from_csv(cls, path, sep, original, *additional):
        t = super().from_csv(path, sep, original)

        try:
            t.normalized = pd.read_csv(path+additional[0], sep=sep)
            return t
        except Exception as e:
            print(f'Error reading CSV!')