import pandas as pd


data_rename_map = {'id': 'tweet_id',
                   'text': 'text_orig',
                   'entities.mentions':'mentions',
                   'geo.place_id':'place_id'}
users_rename_map = {'id': 'author_id', 'location': 'user_location'}
places_rename_map = {'id': 'place_id', 'full_name': 'location'}


class TwitterData:
    def __init__(self, data):
        self.original = pd.json_normalize(data)

    def rename(self, changes):
        try:
            self.original.rename(columns=changes, inplace=True)
        except KeyError as e:
            print(f"Failed to identify passed columns to rename! \n{e.args[0]}")

    def append(self, other):
        assert isinstance(other, type(self)), 'Cannot append data!'

    def save_csv(self, prefix, lang, topic, num):
        print(self.original.shape[0])

        path = f'{prefix}/{lang}' \
               f'-{topic.upper()}' \
               f'-original' \
               f'-{self.__class__.__name__.lower()}' \
               f'-{self.original.shape[0]}' \
               f'-{num}.csv'
        print(path)

        self.original.to_csv(path, sep='~', index=False)