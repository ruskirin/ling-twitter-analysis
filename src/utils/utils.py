import logging.config
import yaml
import regex as re
from datetime import datetime
from pathlib import Path
from numpy import array_split, ceil
from pandas import read_csv, read_excel, DataFrame
from pandas.errors import ParserError # Custom error


logger = logging.getLogger(__name__)


def get_verb_conjugations() -> DataFrame:
    conf = get_config('g')
    p = get_project_root()/conf['file_paths']['verb_conjug']

    return read_excel(p)


def get_project_root() -> Path:
    """
    Returns the path to the folder
      .../lin-que-dropping/
    """

    path = str(Path(__file__))

    root_pat = r'(.*\/lin-que-dropping)'
    match = re.search(root_pat, path)

    if match is None:
        raise Exception('.../lin-que-dropping not found in path?')

    root_path = Path(match[0])

    return root_path


def get_relative_to_proot(path) -> Path:
    """Get the @path starting from 'lin-que-dropping/..."""
    if not isinstance(path, Path):
        path = Path(path)

        if (not path.is_file()) and (not path.is_dir()):
            raise ValueError(f'"{path}" is not a valid path')

    return path.relative_to(get_project_root())


def get_save_location(loc: str,
                      corpus: str = '',
                      is_test: bool = False,
                      lang: str = 'es') -> Path:
    """
    Get filepath to the saved tweets

    :param loc: 'e' extracted, 'c' cleaned, 'p' processed
    :param corpus: 'twitter' or 'corpes'
    :param is_test: get the test saves
    :param lang: language of tweets (separated by language); default is just the saved
      folder
    """
    dirs = {'e': 'extracted', 'c': 'cleaned', 'p': 'processed'}
    from_locs = {'twitter', 'corpes'}

    data_path = get_project_root()/'data'
    if loc not in dirs.keys():
        return data_path

    data_path = data_path / dirs[loc] / ('saved' if not is_test else 'test')
    if (corpus not in from_locs) or is_test:
        return data_path

    data_path = data_path / corpus
    if lang is None:
        return data_path

    return data_path/lang


def get_saved_data_path(loc,
                        folder=None,
                        corpus='twitter',
                        is_test=False,
                        lang='es') -> list[Path]:
    """
    Get path (or a list of paths) to a directory with saved data

    :param loc: save data location: one of
      {e: extraction, c: cleaning, p: processing}
    :param folder: (optional) folder name to retrieve OR 'all' for all folders
      in save directory
    :param corpus: extraction corpus: one of {twitter, corpes}
    :param is_test: whether to use the test folder
    :param lang: language; one of {es, pt}
    :return: Path object
    """
    if loc not in {'e', 'c', 'p'}:
        raise ValueError(f'"{loc}" must be one of: "e", "c", "p"')

    dir = get_save_location(loc, corpus, is_test, lang)
    if folder is not None:
        if folder == 'all':
            return list(dir.iterdir())

        path = dir/folder
        return path if path.is_dir() else None

    return _select_folders(dir)


def _select_folders(dir: Path) -> list[Path]:
    """
    Prompts you to choose one or more folders from a directory

    :param dir: a `Path` object representing the directory to select folders from
    :return: list of `Path` objects representing selected folders
    """
    if not dir.is_dir():
        raise NotADirectoryError(f'"{dir}" is not a directory')

    d = list(dir.iterdir())

    print(f'Choose from the available folders, comma-separated (or "a" for all):')
    for i, f in enumerate(d):
        print(f'{i}. {f.stem}')

    while True:
        choice = input('Return folder(s): ')
        if choice == 'a':
            return d

        try:
            cs = choice.split(',')
            return [d[int(c.strip())] for c in cs]

        except IndexError:
            print('Select valid folder(s) (or "a" for all)')


def make_dir(dir_path,
             name_scheme,
             date: str = None) -> Path:
    """
    Create a new directory in @dir_path

    :param dir_path: path to directory in which to make another folder
    :param name_scheme: name for the new directory
    :param date: if not None, place new directory inside a folder named as
      datetime string
    :return: Path object to new directory
    """
    if not dir_path.is_dir():
        raise NotADirectoryError('@dir_path must be a directory')
    if date is not None:
        name_scheme = f'{date}/{name_scheme}'

    try:
        path = Path(f'{dir_path}/{name_scheme}')
        path.mkdir(parents=True, exist_ok=True)

        return path
    except FileNotFoundError as fnf:
        logger.exception(f'Cannot make directory -- invalid path: \n{fnf.args}')
    except Exception as e:
        logger.exception(f'Error while making directory! \n{e.args}')


# def get_csv(
#         path: Path,
#         corpus: str,
#         dtypes: dict = None,
#         dates=False,
#         sep='~',
#         lineterminator=None):
#
#     # TODO: attempting to optimize the function has resulted in it breaking
#     #   more often than not. Run and see the results
#
#     """
#     Optimized version utilizing pandas.read_csv() with dtypes specified
#
#     :param path: path to CSV
#     :param corpus: 'twitter' or 'corpes'
#     :param dtypes: (optional) dictionary mapping cols to their respective dtypes
#     :param dates: (optional) list with datetime columns
#     :param sep: separator used in CSV (default: '~')
#     :param lineterminator: (optional) use '\n' if failing to read CSVs
#     :return: dataframe
#     """
#     if corpus not in {'twitter', 'corpes'}:
#         logger.exception(f'{corpus} is not a valid corpus')
#         return None
#
#     conf = get_config()
#
#     if dtypes is None:
#         dtypes = conf['dtypes'][corpus]['regular']
#
#     try:
#         data = read_csv(path,
#                         sep=sep,
#                         dtype=dtypes,
#                         parse_dates=dates,
#                         lineterminator=lineterminator,
#                         on_bad_lines='warn')
#     except ParserError:
#         data = read_csv(path,
#                         sep=sep,
#                         dtype=dtypes,
#                         parse_dates=dates,
#                         lineterminator='\n',
#                         engine='python',
#                         on_bad_lines='error')
#     except TypeError:
#         data = read_csv(path,
#                         sep=sep,
#                         dtype=dtypes,
#                         parse_dates=dates,
#                         lineterminator=lineterminator,
#                         on_bad_lines='warn',
#                         engine='python')
#     except Exception as e:
#         logger.debug(f'Exception while reading CSV: '
#                       f'\n{e.args}')
#         logger.debug(f'\nReading using default dtypes')
#
#         err_dtypes = conf['dtypes'][corpus]['error']
#
#         data = read_csv(path,
#                         sep=sep,
#                         dtype=err_dtypes,
#                         parse_dates=dates,
#                         lineterminator=lineterminator,
#                         on_bad_lines='warn',
#                         engine='python')
#
#         # TODO: keep track of bad lines and record them
#
#         # logger.debug(f'Read {data.shape[0]} tweets; removing null entries')
#
#         # bad_text = data['normalized'].isna()
#         # data = data.drop(data[bad_text].index).reset_index(drop=True)
#         #
#         # logger.debug(f'Found {bad_text.sum()} bad text entries. Returning '
#         #               f'remaining {data.shape[0]}.')
#
#     return data


# def save_csv(
#         path: Path,
#         df: DataFrame,
#         name_scheme,
#         batch=False,
#         batch_size=1000,
#         file_sep='~'):
#     """
#     Save a dataframe as a CSV; option to batch into multiple files
#
#     :param path: location to save
#     :param df: dataframe
#     :param name_scheme: filename
#     :param batch: batch the dataframe? (bool)
#     :param batch_size: batch size if batch == True
#     :param file_sep: CSV separator
#     """
#     try:
#         if not batch:
#             name = f'{name_scheme}-{df.shape[0]}'
#             if name_scheme[-4:] != '.csv':
#                 name = name + '.csv'
#
#             df.to_csv(path / name, sep=file_sep, index=False)
#             logger.info(f'Saved dataframe ({name_scheme}) CSV into: {path}')
#         else:
#             bins = ceil(df.shape[0] / batch_size)
#             # Split into batches of approximately the specified batch size
#             for i, b in enumerate(array_split(df, bins)):
#                 name = f'{name_scheme}-{i}-{b.shape[0]}.csv'
#                 b.to_csv(path / name, sep=file_sep, index=False)
#
#             logger.info(f'Saved {bins} files into: {path}')
#     except Exception as e:
#         logger.exception(e.args)
#         print(f'Problems saving data to CSV!')


def save_excel(
        path: Path,
        df: DataFrame,
        name_scheme,
        batch=False,
        batch_size=1000):
    try:
        if not batch:
            name = name_scheme
            if '.xlsx' not in name_scheme:
                name = name + '.xlsx'

            df.to_excel(path / name, index=False)
            logger.info(f'Saved dataframe ({name_scheme}) xlsx into: {path}')
        else:
            bins = ceil(df.shape[0] / batch_size)
            # Split into batches of approximately the specified batch size
            for i, b in enumerate(array_split(df, bins)):
                name = f'{name_scheme}-{i}-{b.shape[0]}.xlsx'
                b.to_excel(path / name, index=False)

            logger.info(f'Saved {bins} files into: {path}')
    except Exception as e:
        logger.exception(e.args)
        print(f'Problems saving data as .xlsx!')


def save_readme(location, text, append=True):
    op = 'a+' if append else 'w'

    with open(location/'README.txt', op) as readme:
        readme.seek(0)

        if append and (len(readme.read(100)) > 0):
            text = '\n\n' + text

        readme.write(text)


def get_str_datetime_now(date: bool=True, time: bool=True):
    """
    Get datetime at time of this function's call; used for directory and file
    naming. Format used is defined in the general config. Will return None if
    both date and time are false

    :param date: include date?
    :param time: include time?
    :return: datetime string representation
    """
    conf = get_config()

    if date and time:
        fmt = f'{conf["formats"]["date"]}-at-{conf["formats"]["time"]}'
    elif date and (not time):
        fmt = f'{conf["formats"]["date"]}'
    elif (not date) and time:
        fmt = f'{conf["formats"]["time"]}'
    else:
        return None

    return datetime.now().strftime(fmt)


def setup_logger(file_name: str, desc: str = '', append=False):
    """
    Create and configure the root logger with 3 handlers: console, detail,
      and progress. Logs saved to .../lin-que-dropping/logs/ .

    :param file_name: short name to describe session (used in log file names)
    :param desc: readme description of log
    :param append: append to existing log if exists?
    :return: Nothing -- loggers are singletons and can be accessed using
      logger.getLogger(logger_name)
    """

    try:
        time = datetime.now()
        # Logs partitioned by date; create directory (if it doesn't exist)
        dir_name = get_str_datetime_now(True, False)
        log_dir_path = get_project_root() / 'logs'
        log_path = make_dir(log_dir_path, dir_name)

        conf = get_config('l')
        logger = logging.getLogger()

        if logger.hasHandlers():
            # logger already exists
            print('Root logger has handlers')
            return None

        logger.setLevel(logging.DEBUG)
        logger.propagate = False

        # Remove existing handlers (found they stuck between sessions)
        while logger.hasHandlers():
            logger.removeHandler(logger.handlers[0])

        f = conf['formatters']['def']
        formatter = logging.Formatter(style=f['style'],
                                      fmt=f['format'],
                                      datefmt=f['datefmt'])

        # Console handler
        ch = conf['handlers']['console']
        console_hand = logging.StreamHandler()
        console_hand.setLevel(ch['level'])
        console_hand.setFormatter(formatter)

        # Detail handler; records DEBUG and up
        dh = conf['handlers']['detail']
        dh_name = log_path/dh['filename'].format(
            time.strftime("%H:%M:%S"), file_name
        )
        detail_hand = logging.FileHandler(
            filename=dh_name.as_posix(),
            mode='a' if append else 'w'
        )
        detail_hand.setLevel(dh['level'])
        detail_hand.setFormatter(formatter)
        logger.addHandler(detail_hand)

        # Progress handler; records INFO and up
        ih = conf['handlers']['progress']
        ih_name = log_path/ih['filename'].format(
            time.strftime("%H:%M:%S"), file_name
        )
        prog_hand = logging.FileHandler(
            filename=ih_name.as_posix(),
            mode='a' if append else 'w'
        )
        prog_hand.setLevel(ih['level'])
        prog_hand.setFormatter(formatter)
        logger.addHandler(prog_hand)

        # Update log README
        save_readme(log_path, f'***** {dh_name.stem} *****'
                              f'\n{desc}')

    except Exception as e:
        print(f'Error getting logger! \n{e.args}')
        return None


def get_yaml(path: Path):
    """Return the contents of a yaml file"""
    try:
        with open(path, 'r') as f:
            logger.info(f'Opened config file: {path.stem}')
            return yaml.safe_load(f)

    except TypeError as e:
        logger.exception(f'Failed to open config file!\n{e.args}')
        raise


def get_config(conf_type='g') -> dict|None:
    """
    Return a YAML configuration dict.
    :param conf_type: choice of 'g, l, e, c, p'
      for 'general, log, extraction, cleaning, processing' configurations files
      (default: 'g')
    """
    types = {'g': 'general',
             'conn': 'connection',
             'l': 'log',
             'e': 'extraction',
             'c': 'cleaning',
             'p': 'processing'}

    if conf_type not in types:
        raise ValueError(f'Must pass valid @conf_type; one of: \n{types.items()}')

    try:
        config_path = get_project_root()/'config'/f'{types[conf_type]}_config.yml'
        return get_yaml(config_path)
    except Exception as e:
        logger.exception(f'Failed to open config file! {e.args}')
        raise


def get_updated_config(conf: dict, path: Path) -> dict:
    """
    Update and return a config file
    """
    try:
        with open(path, 'w') as f:
            yaml.dump(conf, f)

        return get_config(path=path)

    except Exception as e:
        logger.exception(f'Failed to update config file!\n >>> {e.args} <<<')
        return conf
    finally:
        logger.info(f'Updated config file at: {path}')


def remove_empty_dirs(path: Path):
    """
    Remove empty directories in @path
    """
    removed = []

    for f in path.iterdir():
        if f.is_dir():
            # pathlib.Path.rmdir() throws OSError if attempting to remove
            #   non-empty directory
            try:
                f.rmdir()
                removed.append(f.name)
            except OSError:
                continue

    logger.info(f'Removed empty directories in {str(path)}: \n{removed}')
    print(removed)


if __name__ == '__main__':
    print(list(Path().iterdir()))
    print(get_relative_to_proot(get_project_root()/'src'/'twitter_connection/twitter_data'))