import pandas as pd
from logging import getLogger

from twitter_data import twitter_data


class Places(twitter_data.TwitterData):
    def __init__(self, data):
        super().__init__(data)

        self.rename_cols(twitter_data.conf['rename_maps']['places'])

    def append(self, other):
        # TODO: method is duplicated across all the twitter_data objects;
        #   move to twitter_data parent object
        try:
            super().append(other)

            self.data = pd.concat(
                [self.data, other.data],
                axis=0,
                ignore_index=True
            )
        except Exception as e:
            print(f'Failed to append data! {e.args[0]}')