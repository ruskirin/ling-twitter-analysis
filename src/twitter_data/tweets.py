import re
from logging import getLogger

import pandas as pd
from emoji import replace_emoji
from unidecode import unidecode
from twitter_data import TwitterData
import files


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
    s1 = files.get_save_path('e')
    s2 = files.get_save_path('e') / '2021-11-07-at-22:10:00'
    p1 = s2 / 'es-parecer-original-tweets-10083-0.csv'

    d = Tweets.from_csv(p1, 'es')
    dp = d.save(s1, 'csv', name_scheme='sample')[0]

    d2 = Tweets.from_csv(dp, 'es', topic='sample')

    print(d.data.equals(d2.data))