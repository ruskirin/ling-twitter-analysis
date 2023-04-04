import re
from logging import getLogger
import pandas as pd
from emoji import replace_emoji
from unidecode import unidecode
from twitter_data import TwitterData


logger = getLogger(__name__)


class Tweets(TwitterData):
    def __init__(self, data: pd.DataFrame, topic: str, lang: str):
        super().__init__(data, topic, lang)

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


if __name__ == '__main__':
    pass