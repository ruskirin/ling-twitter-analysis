import pandas as pd
from logging import getLogger
from twitter_data.twitter_data import TwitterData, conf


class Users(TwitterData):
    def __init__(self, data=None):
        super().__init__(data)

        if data is not None:
            self.rename_cols(conf['rename_maps']['users'])

    def append(self, other):
        try:
            super().append(other)

            if self.data is None:
                self.data = other.data

                self.rename_cols(conf['rename_maps']['users'])

            else:
                self.data = pd.concat(
                    [self.data, other.data],
                    axis=0,
                    ignore_index=True
                )

        except Exception as e:
            print(f'Failed to append data! {e.args[0]}')