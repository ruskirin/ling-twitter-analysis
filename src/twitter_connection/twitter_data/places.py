import re
import logging
import pandas as pd

from twitter_data import twitter_data


class Places(twitter_data.TwitterData):
    def __init__(self, data):
        super().__init__(data)

        self.rename(twitter_data.conf['rename_maps']['places'])

    def append(self, other):
        try:
            super().append(other)

            self.data = self.data.append(
                other.data, ignore_index=True)
        except Exception as e:
            print(f'Failed to append data! {e.args[0]}')

    def save_csv(self, path, lang, topic, num):
        self.remove_dups('place_id')
        super().save_csv(path, lang, topic, num)