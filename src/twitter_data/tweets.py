import re
from logging import getLogger
from emoji import replace_emoji
from unidecode import unidecode
from src.twitter_data.twitter_data import TwitterData
import files

logger = getLogger(__name__)


class Tweets(TwitterData):
    def __init__(self, data, topic, lang):
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
    path = files.choose_save_path('e')[0] \
           / 'tweets' \
           / 'es-twitter-decir-tweets-1-589.csv'
    d = Tweets.from_csv(path, 'es')

    d.update_ids()