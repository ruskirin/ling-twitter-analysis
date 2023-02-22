import re
from logging import getLogger
import pandas as pd
from emoji import replace_emoji
from unidecode import unidecode
from pathlib import Path
from twitter_data.twitter_data import TwitterData, conf, gconf


logger = getLogger(__name__)


class Tweets(TwitterData):
    def __init__(self, data, topic, lang):
        super().__init__(data, topic, lang)

        # if data is not None:
        #     # Change column names
        #     self.rename(conf['rename_maps']['tweets'])
        #     # Append a column with normalized text
        #     self.data = pd.concat([self.data, self.normalize(data)], axis=1)

    def normalize(self, data):
        return data.loc[:, 'text_orig']\
            .apply(self.norm_text)\
            .apply(unidecode)\
            .rename_cols('text_norm')

    # Must precede unidecode otherwise text formatting might cause issues
    @staticmethod
    def norm_text(tweet):
        t = re.sub(r'@[\w]+[\b ]*', '', tweet)
        t = re.sub(r'[\t]+|[\n]+', ' ', t)
        # Props to: https://stackoverflow.com/a/50602709/13557629
        return replace_emoji(r'', t)

    # TODO: old extension of from_csv() from previous version. Normalization
    #   was performed prior to saving raw data... necessary?
    #
    # @classmethod
    # def from_csv(cls, path, sep, original, *additional):
    #     t = super().from_csv(path, sep, original)
    #
    #     try:
    #         t.normalized = pd.read_csv(path+additional[0], sep=sep)
    #         return t
    #     except Exception as e:
    #         print(f'Error reading CSV!')