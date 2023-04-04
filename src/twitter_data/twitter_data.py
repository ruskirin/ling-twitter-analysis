import pandas as pd
from pandas.errors import ParserError
import csv
from logging import getLogger
from pathlib import Path
from numpy import ceil, array_split
from datetime import datetime
import files
import configs


# TODO 2/23: not sure if this should stay global here, be made into an
#  attribute, or become a global variable elsewhere
logger = getLogger(__name__)
conf = configs.get_yaml(files.get_project_root() / 'config' / 'twitterdata.yml')
gconf = configs.read_conf()


# TODO 2/23: perhaps extend pandas.DataFrame instead of creating a wrapper
#   around it?
class TwitterData:
    def __init__(self, data: pd.DataFrame, topic: str, lang: str):
        self.dtype = type(self).__name__.lower()
        self.data = data

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

        # TODO 2/28: will not work with extracted 'twitterdata' files as those
        #   do not have an 'id' column. Handle those cases!

        # TODO 3/10: _remove_dups() removed from __init__(); see when/where it
        #   should be used; NOTE: not necessary for update_ids() as
        #   _remove_ids() only looks at unique ids from self.data -- duplicates
        #   are not double registered

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
            self._write_ids(dids, path=id_path)
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
        with open(path, newline='') as f:
            reader = csv.reader(f, delimiter=',', quotechar='"')
            ids = set([r for r in reader][0])
            return ids

    def _write_ids(self, ids: set, path: Path):
        """Save the set of ids"""
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

    def save(
            self,
            path: Path,
            save_format,
            name_scheme=None,
            batch=False,
            batch_size=1000,
            batch_num=None,
            sep_by_type=False) -> list[Path]:
        """
        Save @self.data into data format specified by @save_format:
          {filename format}
          -{batch number(optional)}
          -{dataframe/batch size}.xlsx

        :param path: location to save
        :param save_format: one of {"csv", "excel"}
        :param name_scheme: (optional) alternate filename to use when saving
        :param batch: (def: False) separate dataframe into batches?
        :param batch_size: (if @batch == True) batch size
        :param batch_num: (optional) batch number to append to filename
        :param sep_by_type: (def: False) save into its type directory
          (ie. tweets, twitterdata, etc)
        :return: path of saved data
        """

        try:
            save_paths = []
            data_type = type(self).__name__.lower()
            sep = gconf['csv_sep']

            if sep_by_type:
                path = files.make_dir(path, data_type)

            if not batch:
                name = self._format_filename(
                    name_scheme, save_format, batch_num, batch_size
                )
                if save_format == 'csv':
                    self.data.to_csv(path / name, sep=sep, index=False)
                else:
                    self.data.to_excel(path / name, index=False)

                logger.info(f'Saved dataframe ({name_scheme}) excel sheet into: '
                            f'{files.get_relative_to_proot(path)}')

                save_paths.append(path/name)

            else:
                bins = ceil(self.shape[0] / batch_size)
                # Split into batches of approximately the specified batch size
                for i, b in enumerate(array_split(self.data, bins)):
                    name = self._format_filename(
                        name_scheme, save_format, i, b.shape[0]
                    )
                    if save_format == 'csv':
                        b.to_csv(path / name, sep=sep, index=False)
                    else:
                        b.to_excel(path / name, index=False)

                    save_paths.append(path/name)

                logger.info(f'Saved {bins} dataframes into: '
                            f'{files.get_relative_to_proot(path)}')

            return save_paths

        except KeyError as e:
            logger.exception(f'Invalid key in config file:\n{e.args}')
            raise
        except Exception as e:
            logger.exception(f'Problems saving data to {save_format}:\n{e.args}')
            raise

    def _format_filename(
            self,
            name_scheme: str,
            save_format: str,
            batch_num: int|None,
            batch_size: int|None):
        """
        Create a standardized filename to use for saving data
        :param name_scheme:
        :param save_format: one of {"csv", "excel"}
        :param batch_num:
        :param batch_size:
        """
        data_type = type(self).__name__.lower()
        if save_format == 'csv':
            ext = '.csv'
        elif save_format == 'excel':
            ext = '.xlsx'
        else:
            raise ValueError(f'Invalid save format ({save_format})')

        if name_scheme is None:
            # format the filename as specified in config file
            name_scheme = conf['save_file']['twitter'].format(
                lang=self.lang,
                verb=self.topic,
                data_type=data_type
            )

        # Remove the file format to append dataframe size
        name_scheme = Path(name_scheme).stem

        if batch_num is not None:
            name_scheme = f'{name_scheme}-{batch_num}'

        if batch_size is not None:
            name_scheme = f'{name_scheme}-{self.shape[0]}'

        return name_scheme + ext

    @classmethod
    def from_csv(cls,
                 path: Path,
                 lang,
                 topic=None,
                 subset: list = None,
                 dtypes: dict = None,
                 dates: list = None,
                 lineterminator=None):
        """
        Optimized version utilizing pandas.read_csv() with dtypes specified

        :param path: path to CSV
        :param lang: language of dataset ('es' or 'pt')
        :param topic: (optional) name to give the dataframe; used when writing
          to file
        :param subset: (optional) subset of the columns to return
        :param dtypes: (optional) dict of {column: dtype}; used to pass custom
          dtype specs for "unconventional" dataframes
        :param dates: (optional) list of date columns to parse
        :param lineterminator: (optional) use '\n' if failing to read CSVs
        :return: dataframe
        """
        date_formats = conf['date_formats']
        sep = gconf['csv_sep']

        if dtypes is None:
            dtypes = conf['dtypes']['twitter']['regular']
        if dates is None:
            dates = conf['dtypes']['twitter']['dates']

        if topic is None:
            # Attempt to identify topic (aka verb of interest) from filename
            topic = extract_verb_from_filename(path)
            if topic is None:
                raise ValueError(f'Cannot identify dataframe topic from '
                                 f'filename -- pass into @topic')

        data = pd.read_csv(
            path,
            usecols=subset,
            dtype=dtypes,
            sep=sep,
            lineterminator=lineterminator,
            on_bad_lines='warn'
        )

        try:
            data.loc[:, dates] = data.loc[:, dates].apply(
                pd.to_datetime, format=date_formats['cleaned']
            )

            return cls(data, topic, lang)
        except ParserError:
            data.loc[:, dates] = data.loc[:, dates].apply(
                pd.to_datetime, format=date_formats['extracted']
            )

            return cls(data, topic, lang)
        except ValueError as e:
            logger.exception(f'Passed an invalid argument: \n{e.args}')
            raise

    @classmethod
    def from_json(cls, json_data, topic, lang):
        return cls(pd.json_normalize(json_data), topic, lang)


def convert_dtypes(df: pd.DataFrame, type_map: dict) -> pd.DataFrame:
    # TODO 4/3/2023: see if method is necessary - if so, update

    """Assign appropriate dtypes to each column"""
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
    s1 = files.get_save_path('e')
    s2 = files.get_save_path('e')/'2021-11-07-at-22:10:00'
    p1 = s2/'es-parecer-original-tweets-10083-0.csv'

    d = TwitterData.from_csv(p1, 'es')
    d.save(s1/'sample.xlsx', 'excel')

    d2 = TwitterData.from_csv(p1, 'es')

    print(d.data['created_at'])
    print(d.data.info())