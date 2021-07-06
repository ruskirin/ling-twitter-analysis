import re
import pandas as pd
import emoji

from unidecode import unidecode
from twitter_data import twitter_data


class Tweets(twitter_data.TwitterData):
    def __init__(self, data):
        super().__init__(data)

        self.rename(twitter_data.data_rename_map)

        self.normalized = self.normalize()

    def normalize(self):
        return pd.concat([self.original.loc[:, 'tweet_id'],
                          self.original.loc[:, 'text_orig']
                             .apply(self.clean)
                             .apply(unidecode)
                             .rename('text_normd')
                          ],
                          axis=1)

    # Must precede unidecode otherwise text formatting might cause issues
    @staticmethod
    def clean(tweet):
        t = re.sub(r'@[\w]+[\b ]*', '', tweet)
        # Props to: https://stackoverflow.com/a/50602709/13557629
        return emoji.get_emoji_regexp('es').sub(r'', t)

    def set_tokenized(self, tokenized):
        if self.normalized.shape[0]==0:
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
            self.normalized = self.normalized.append(
                other.normalized, ignore_index=True)
        except Exception as e:
            print(f'Failed to append data! {e.args[0]}')

    def save_csv(self, prefix, lang, topic, num):
        try:
            super().save_csv(prefix, lang, topic, num)

            path = f'{prefix}/{lang}' \
                   f'-{topic.upper()}' \
                   f'-normalized' \
                   f'-{self.__class__.__name__.lower()}' \
                   f'-{self.normalized.shape[0]}' \
                   f'-{num}.csv'
            self.normalized.to_csv(path, sep='~', index=False)
        except Exception as e:
            print(f'Problems saving data to CSV! '
                  f'>>>\n {e.args} \n<<<')
