import regex as re
import pandas as pd
import csv
from logging import getLogger
from pathlib import Path
from numpy import ceil, array_split
import files
import configs
import pytest


# TODO 2/23: not sure if this should stay global here, be made into an
#  attribute, or become a global variable elsewhere
logger = getLogger(__name__)
conf = configs.get_yaml(files.get_project_root() / 'config' / 'twitterdata.yml')
gconf = configs.read_conf()


# TODO 2/23: perhaps extend pandas.DataFrame instead of creating a wrapper
#   around it?
class TwitterData:
    def __init__(self,
                 data: pd.DataFrame,
                 topic: str,
                 lang: str):

        self.dtype = type(self).__name__.lower()
        self.data = self._remove_dups(data)

        # dataframe name used when saving and retrieving
        self.topic = topic
        self.lang = lang

    @property
    def shape(self):
        return self.data.shape

    def set_topic(self, topic, lang):
        name = conf['save_file']['twitter'].format(topic, lang)
        self.topic = name

    def update_ids(self, id_path: Path = None):

        # TODO 2/28: add some exception handling here. eg. verify that @id_path
        #   points to a valid file

        """
        Update the global set of ids as specified in the general configuration
          (if @id_path is None, otherwise update the set in @id_path).
        :param id_path: (optional) Path object to CSV file holding set of ids
        """
        if id_path is None:
            id_path = files.get_project_root() \
                      / gconf['file_paths']['twitter_ids'] \
                      / (self.dtype + '.csv')

        try:
            gids = self._read_ids()
            data = self._remove_ids(gids)

            dids = set(data['id'].unique())
            self._write_ids(dids)
            self.data = data

        except Exception as e:
            logger.exception(e.args)
            raise

    def _remove_ids(self, ids: set):
        """
        Remove entries from @self.data where 'id' is in @ids
        """
        d_ids = set(self.data['id'].unique().astype(str))

        dup_ids = ids.intersection(d_ids)

        logger.debug(f'Found {len(dup_ids)} existing entries.')

        dups = self.data.loc[self.data['id'].isin(dup_ids), :].index
        df = self.data.drop(index=dups).reset_index(drop=True)

        logger.debug(f'Returning {self.dtype} with {df.shape[0]} unique entries.')

        return df

    def _remove_dups(self, data: pd.DataFrame, subset='id'):
        """Remove duplicated entries from @data dataframe"""
        d = data.drop_duplicates(subset=subset, ignore_index=True)
        diff = data.shape[0] - d.shape[0]

        logger.debug(f'Removed {diff} duplicated {self.dtype} entries')
        return d

    def _read_ids(self, path: Path = None) -> set:
        """Read the tallied data ids"""
        if path is None:
            path = files.get_project_root() \
                / gconf['file_paths']['twitter_ids'] \
                / (self.dtype + '.csv')

        with open(path, newline='') as f:
            reader = csv.reader(f, delimiter=',', quotechar='"')
            ids = set([r for r in reader][0])
            return ids

    def _write_ids(self, ids: set, path: Path = None):
        """Save the set of ids"""
        if path is None:
            path = files.get_project_root() \
                   / gconf['file_paths']['twitter_ids'] \
                   / (self.dtype + '.csv')

        ids = list(ids)

        with open(path, 'w', newline='') as f:
            writer = csv.writer(f, delimiter=',', quotechar='"')
            writer.writerow(ids)

            logger.info(f'Updated {self.dtype} ids; total: {len(ids)}')

    def rename_cols(self, changes):
        try:
            self.data.rename(columns=changes, inplace=True)
        except KeyError as e:
            print(f"Failed to identify passed columns to rename! \n{e.args[0]}")

    def append(self, other):
        assert isinstance(other, type(self)), 'Cannot append data!'

        try:
            self.data = pd.concat(
                [self.data, other.data],
                axis=0,
                ignore_index=True
            )
        except Exception as e:
            print(f'Failed to append data! {e.args[0]}')

    def save_csv(
            self,
            path: Path,
            batch_data=False,
            batch_size=1000,
            batch=None,
            sep_by_type=False,
            name_scheme=None):
        """
        Save a dataframe as a CSV; option to batch into multiple files

        :param path: location to save
        :param batch_data: batch the dataframe? (bool)
        :param batch_size: batch size if batch == True
        :param batch: (optional) batch number to append to filename
        :param name_scheme: (optional) alternate name to use when saving
        """

        # TODO 2/21: split this method up; way past having a single
        #  responsibility. Also the separate batching and also single saving
        #  using @page seems excessive. Thinking perhaps create separate methods
        #  for different types of operations? eg. extraction_save(), clean_save(),
        #  etc...?

        try:
            sep = gconf['csv_sep']
            data_type = type(self).__name__.lower()

            if sep_by_type:
                path = files.make_dir(path, data_type)

            if name_scheme is None:
                # format the filename as specified in config file
                name_scheme = conf['save_file']['twitter'].format(
                    lang=self.lang,
                    verb=self.topic,
                    data_type=data_type
                )

            # Remove the file format for now as need to append dataframe size
            if name_scheme[-4:] == '.csv':
                name_scheme = name_scheme[:-4]

            if batch is not None:
                name_scheme = f'{name_scheme}-{batch}'

            if not batch_data:
                name = f'{name_scheme}-{self.shape[0]}.csv'
                self.data.to_csv(path / name, sep=sep, index=False)
                logger.info(f'Saved dataframe ({name_scheme}) CSV into: '
                            f'{files.get_relative_to_proot(path)}')
            else:
                bins = ceil(self.shape[0] / batch_size)
                # Split into batches of approximately the specified batch size
                for i, b in enumerate(array_split(self.data, bins)):
                    name = f'{name_scheme}-{i}-{b.shape[0]}.csv'
                    b.to_csv(path / name, sep=sep, index=False)

                logger.info(f'Saved {bins} dataframes into: '
                            f'{files.get_relative_to_proot(path)}')
        except KeyError as e:
            logger.exception(f'Invalid key in config file:\n{e.args}')
            raise
        except ValueError as e:
            logger.info(f'Saved file(s) into: .../{path.name}')
        except Exception as e:
            logger.exception(f'Problems saving data to CSV:\n{e.args}')
            raise

    @classmethod
    # def save_excel(
    #         cls,
    #         path: Path,
    #         df: DataFrame,
    #         name_scheme,
    #         batch=False,
    #         batch_size=1000):
    #     try:
    #         if not batch:
    #             name = name_scheme
    #             if '.xlsx' not in name_scheme:
    #                 name = name + '.xlsx'
    #
    #             df.to_excel(path / name, index=False)
    #             logger.info(
    #                 f'Saved dataframe ({name_scheme}) xlsx into: {path}'
    #                 )
    #         else:
    #             bins = ceil(df.shape[0] / batch_size)
    #             # Split into batches of approximately the specified batch size
    #             for i, b in enumerate(array_split(df, bins)):
    #                 name = f'{name_scheme}-{i}-{b.shape[0]}.xlsx'
    #                 b.to_excel(path / name, index=False)
    #
    #             logger.info(f'Saved {bins} files into: {path}')
    #     except Exception as e:
    #         logger.exception(e.args)
    #         print(f'Problems saving data as .xlsx!')

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
    verbs = files.get_verb_conjugations().loc[:, 'verb'].to_numpy()
    stem = path.stem

    for v in verbs:
        if v in stem:
            return v

    return None


if __name__ == '__main__':
    path = files.choose_save_path('e')[0] \
           / 'tweets' \
           / 'sample-es-twitter-decir-tweets-1-589.csv'
    d = TwitterData.from_csv(path, 'es')

    d.update_ids()