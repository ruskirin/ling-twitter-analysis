import re
import logging
import pandas as pd

from twitter_data import twitter_data


class Users(twitter_data.TwitterData):
    def __init__(self, data=None):
        super().__init__(data)

        if data is not None:
            self.rename(twitter_data.conf['rename_maps']['users'])

    def append(self, other):
        try:
            super().append(other)

            if self.data is None:
                self.data = other.data

                self.rename(twitter_data.conf['rename_maps']['users'])

            else:
                self.data = pd.concat(
                    [self.data, other.data],
                    axis=0,
                    ignore_index=True
                )

        except Exception as e:
            print(f'Failed to append data! {e.args[0]}')

    def save_csv(self, path, lang, topic, num):
        self.remove_dups('user_id')
        super().save_csv(path, lang, topic, num)