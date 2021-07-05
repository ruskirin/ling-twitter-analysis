import re
import pandas as pd
import emoji

from unidecode import unidecode
from twitter_data import twitter_data


class Tweet(twitter_data.TwitterData):
    def __init__(self, data):
        super().__init__(data)

        self.rename(twitter_data.data_rename_map)

        self.processed = self.normalize()

    def normalize(self):
        return pd.concat([self.original.loc[:, 'tweet_id'],
                          self.original.loc[:, 'text_orig']
                             .apply(self.demoji)
                             .apply(unidecode)
                             .rename('text_normd')
                          ],
                          axis=1)

    # Must precede unidecode otherwise text formatting might cause issues
    def demoji(self, tweet):
        # Props to: https://stackoverflow.com/a/50602709/13557629
        return emoji.get_emoji_regexp('es').sub(r'', tweet)

    def set_tokenized(self, tokenized):
        if self.processed.shape[0]==0:
            print('Tokenizing but Tweet missing normalized text!')

        self.details = {t.text: (t.lemma_, t.pos_, t.tag_)
                        for t in tokenized
                        if (t.pos_ != 'SPACE')}

    def lemmatize(self):
        pass

    def append(self, other):
        try:
            super().append(other)

            self.original = self.original.append(
                other.original, ignore_index=True)
            self.processed = self.processed.append(
                other.processed, ignore_index=True)
        except Exception as e:
            print(f'Failed to append data! {e.args[0]}')
