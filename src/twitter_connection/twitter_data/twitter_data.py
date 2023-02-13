import logging
import regex as re
import pandas as pd
from utils import save_csv as util_save_csv
from utils import get_config, make_dir

conf = get_config('e')
gen_conf = get_config()


class TwitterData:
    def __init__(self, data: pd.DataFrame):
        self.data = data

    def rename(self, changes):
        try:
            self.data.rename(columns=changes, inplace=True)
        except KeyError as e:
            print(f"Failed to identify passed columns to rename! \n{e.args[0]}")

    def append(self, other):
        assert isinstance(other, type(self)), 'Cannot append data!'

    def save_csv(self, path, lang, topic, num):
        data_type = self.__class__.__name__.lower()
        path = make_dir(path, data_type)

        name_scheme = f'{lang}' \
               f'-{topic}' \
               f'-original' \
               f'-{data_type}' \
               f'-{num}'

        util_save_csv(path, self.data, name_scheme)

    def remove_dups(self, subset: str):
        logging.debug(f'Removing duplicates; '
                      f'originally {self.data.shape[0]} entries')
        dups = self.data.duplicated(subset=subset)
        self.data = self.data.drop(self.data[dups].index)

        logging.debug(f'Found {dups.sum()} duplicates.'
                      f'\n{self.data.shape[0]} remaining after drop')

    @classmethod
    def from_csv(cls, path, sep, original, *additional):

        try:
            with open(path+original) as f:
                t = cls(pd.read_csv(f, sep=sep))
                return t
        except Exception as e:
            print(f'Exception while extracting from CSV! {e.args}')
            return None

    @classmethod
    def from_json(cls, json_data):
        return cls(pd.json_normalize(json_data))


def convert_dtypes(df: pd.DataFrame, type_map: dict) -> pd.DataFrame:
    logging.debug(f'Converting dataframe column dtypes as:\n{type_map}')
    drop = []

    for col, dtype in type_map.items():
        if col not in df.columns:
            continue

        try:
            if dtype=='datetime':
                df.loc[:, col] = pd.to_datetime(
                    df.loc[:, col],
                    format=gen_conf['formats']['date'],
                    errors='coerce')
                continue
            elif (dtype=='int') or (dtype=='float'):
                df.loc[:, col] = pd.to_numeric(df.loc[:, col], errors='coerce')
        except Exception as e:
            logging.warning(e.__class__, e.args)

    df.drop(drop, axis=0, inplace=True)
    return df