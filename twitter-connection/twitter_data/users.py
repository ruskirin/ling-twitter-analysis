import re
import pandas as pd

from twitter_data import twitter_data


class Users(twitter_data.TwitterData):
    def __init__(self, data):
        super().__init__(data)

        self.rename(twitter_data.users_rename_map)

    def append(self, other):
        try:
            super().append(other)

            self.original = self.original.append(
                other.original, ignore_index=True)
        except Exception as e:
            print(f'Failed to append data! {e.args[0]}')