import regex as re
import pandas as pd
from logging import getLogger
from pathlib import Path
from numpy import ceil, array_split
import utils as u


logger = getLogger(__name__)
conf = u.get_yaml(u.get_project_root()/'config'/'twitterdata.yml')
gconf = u.get_config()


class TwitterData:
    def __init__(self, data: pd.DataFrame, topic: str, lang: str):
        self.data = data
        # dataframe name used when saving and retrieving
        self.topic = topic
        self.lang = lang

    def set_topic(self, topic, lang):
        name = conf['save_file']['twitter'].format(topic, lang)
        self.topic = name

    def rename_cols(self, changes):
        try:
            self.data.rename(columns=changes, inplace=True)
        except KeyError as e:
            print(f"Failed to identify passed columns to rename! \n{e.args[0]}")

    def append(self, other):
        assert isinstance(other, type(self)), 'Cannot append data!'

    def save_csv(
            self,
            path: Path,
            batch=False,
            batch_size=1000,
            name_scheme=None):
        """
        Save a dataframe as a CSV; option to batch into multiple files

        :param path: location to save
        :param batch: batch the dataframe? (bool)
        :param batch_size: batch size if batch == True
        :param name_scheme: (optional) alternate name to use when saving
        """
        try:
            sep = gconf['csv_sep']
            if name_scheme is None:
                name_scheme = conf['save_file']['twitter'].format(
                    lang=self.lang,
                    verb=self.topic,
                    data_type=type(self).__name__.lower()
                )

            # Remove the file format for now as need to append dataframe size
            if name_scheme[-4:] == '.csv':
                name_scheme = name_scheme[:-4]

            if not batch:
                name = f'{name_scheme}-{self.data.shape[0]}.csv'
                self.data.to_csv(path / name, sep=sep, index=False)
                logger.info(f'Saved dataframe ({name_scheme}) CSV into: '
                            f'{u.get_relative_to_proot(path)}')
            else:
                bins = ceil(self.data.shape[0] / batch_size)
                # Split into batches of approximately the specified batch size
                for i, b in enumerate(array_split(self.data, bins)):
                    name = f'{name_scheme}-{i}-{b.shape[0]}.csv'
                    b.to_csv(path / name, sep=sep, index=False)

                logger.info(f'Saved {bins} dataframes into: '
                            f'{u.get_relative_to_proot(path)}')
        except KeyError as e:
            logger.exception(f'Invalid key in config file:\n{e.args}')
            raise
        except ValueError as e:
            logger.info(f'Saved file(s) into: .../{path.name}')
        except Exception as e:
            logger.exception(f'Problems saving data to CSV:\n{e.args}')
            raise

    @classmethod
    def from_csv(cls,
                 path: Path,
                 lang,
                 topic=None,
                 columns: list = None,
                 lineterminator=None):
        """
        Optimized version utilizing pandas.read_csv() with dtypes specified

        :param path: path to CSV
        :param lang: language of dataset ('es' or 'pt')
        :param topic: name to give the dataframe; used when writing to file
        :param columns: (optional) list of columns to load from CSV
        :param sep: separator used in CSV (default: '~')
        :param lineterminator: (optional) use '\n' if failing to read CSVs
        :return: dataframe
        """
        try:
            dtypes = conf['dtypes']['twitter']['regular']
            dates = conf['dtypes']['twitter']['dates']
            sep = gconf['csv_sep']

            if topic is None:
                # Attempt to identify topic from filename
                topic = extract_verb_from_filename(path)
                if topic is None:
                    raise ValueError(f'Cannot identify dataframe topic from '
                                     f'filename -- pass into @topic')

            data = pd.read_csv(
                path,
                usecols=columns,
                dtype=dtypes,
                parse_dates=dates,
                sep=sep,
                lineterminator=lineterminator,
                on_bad_lines='warn'
            )
            return cls(data, topic, lang)
        except KeyError as e:
            logger.exception(f'Configuration file does not have the passed keys'
                             f'\n{e.args}')
            raise
        except ValueError as e:
            logger.exception(f'Passed an invalid argument: \n{e.args}')
            raise

    @classmethod
    def from_json(cls, json_data, topic, lang):
        return cls(pd.json_normalize(json_data), topic, lang)

    def remove_dups(self, subset: str):
        logger.debug(f'Removing duplicates; '
                      f'originally {self.data.shape[0]} entries')
        dups = self.data.duplicated(subset=subset)
        self.data = self.data.drop(self.data[dups].index)

        logger.debug(f'Found {dups.sum()} duplicates.'
                      f'\n{self.data.shape[0]} remaining after drop')


def convert_dtypes(df: pd.DataFrame, type_map: dict) -> pd.DataFrame:
    logger.debug(f'Converting dataframe column dtypes as:\n{type_map}')
    drop = []

    for col, dtype in type_map.items():
        if col not in df.columns:
            continue

        try:
            if dtype=='datetime':
                df.loc[:, col] = pd.to_datetime(
                    df.loc[:, col],
                    format=gconf['formats']['date'],
                    errors='coerce')
                continue
            elif (dtype=='int') or (dtype=='float'):
                df.loc[:, col] = pd.to_numeric(df.loc[:, col], errors='coerce')
        except Exception as e:
            logger.warning(e.__class__, e.args)

    df.drop(drop, axis=0, inplace=True)
    return df


def extract_verb_from_filename(path: Path):
    """Look for VOI in filename"""
    verbs = u.get_verb_conjugations().loc[:, 'verb'].to_numpy()
    stem = path.stem

    for v in verbs:
        if v in stem:
            return v

    return None


if __name__ == '__main__':
    path = u.get_saved_data_path('e')[0]/'tweets'/'es-acordar-original-tweets-0-597.csv'
    d = TwitterData.from_csv(path, 'es')
    print(d.topic)
    d.save_csv(path.parent,name_scheme='sample.csv')