import re
import pandas as pd

from twitter_data import twitter_data


class Places(twitter_data.TwitterData):
    def __init__(self, data):
        super().__init__(data)

        self.rename(twitter_data.places_rename_map)

    def append(self, other):
        try:
            super().append(other)

            self.original = self.original.append(
                other.original, ignore_index=True)
        except Exception as e:
            print(f'Failed to append data! {e.args[0]}')

    def save_csv(self, prefix, lang, topic, num):
        try:
            super().save_csv(prefix, lang, topic, num)
        except Exception as e:
            print(f'Problems saving data to CSV! '
                  f'>>>\n {[a for a in e.args]}\n<<<')