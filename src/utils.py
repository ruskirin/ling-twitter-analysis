import os
import logging
from logging import config
import yaml
import re
from datetime import datetime
from pathlib import Path
from numpy import array_split
from pandas import read_csv, DataFrame
from numpy import ceil
from pandas.errors import ParserError # Custom error


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


def get_save_path(save_dir: str,
                  save_from: str = '',
                  is_test: bool = False,
                  lang: str = None):
    """
    Get filepath to the saved tweets

    :param save_dir: 'e' extracted, 'c' cleaned, 'p' processed
    :param save_from: 'twitter' or 'corpes'
    :param is_test: get the test saves
    :param lang: language of tweets (separated by language); default is just the saved
      folder
    """
    dirs = {'e': 'extracted', 'c': 'cleaned', 'p': 'processed'}
    from_locs = {'twitter', 'corpes'}

    data_path = get_project_root()/'data'
    if save_dir not in dirs.keys():
        return data_path

    data_path = data_path\
                /dirs[save_dir]\
                /('saved' if not is_test else 'test')
    if (save_from not in from_locs) or is_test:
        return data_path

    data_path = data_path/save_from
    if lang is None:
        return data_path

    return data_path/lang


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
        logging.exception(f'Cannot make directory -- invalid path: \n{fnf.args}')
    except Exception as e:
        logging.exception(f'Error while making directory! \n{e.args}')


def get_csv(
        data_from: str,
        path: Path,
        dtypes: dict = None,
        dates=False,
        sep='~',
        lineterminator=None):

    # TODO: FIX

    """
    Optimized version utilizing pandas.read_csv() with dtypes specified

    :param data_from: 'twitter' or 'corpes'
    :param path: path to CSV
    :param dtypes: (optional) dictionary mapping cols to their respective dtypes
    :param dates: (optional) list with datetime columns
    :param sep: separator used in CSV (default: '~')
    :param lineterminator: (optional) use '\n' if failing to read CSVs
    :return: dataframe
    """

    from_locs = {'twitter', 'corpes'}
    if data_from not in from_locs:
        logging.exception(f'Wrong argument for "data_from": {data_from}'
                          f'\nMust be one of: {from_locs}')
        return None

    conf = get_config()

    if dtypes is None:
        dtypes = conf['dtypes'][data_from]['regular']

    try:
        data = read_csv(path,
                        sep=sep,
                        dtype=dtypes,
                        parse_dates=dates,
                        lineterminator=lineterminator,
                        on_bad_lines='warn')
    except ParserError:
        data = read_csv(path,
                        sep=sep,
                        dtype=dtypes,
                        parse_dates=dates,
                        lineterminator='\n',
                        engine='python',
                        on_bad_lines='error')
    except TypeError:
        data = read_csv(path,
                        sep=sep,
                        dtype=dtypes,
                        parse_dates=dates,
                        lineterminator=lineterminator,
                        on_bad_lines='warn',
                        engine='python')
    except Exception as e:
        logging.debug(f'Exception while reading CSV: '
                      f'\n{e.args}')
        logging.debug(f'\nReading using default dtypes')

        err_dtypes = conf['dtypes'][data_from]['error']

        data = read_csv(path,
                        sep=sep,
                        dtype=err_dtypes,
                        parse_dates=dates,
                        lineterminator=lineterminator,
                        on_bad_lines='warn',
                        engine='python')

        # TODO: keep track of bad lines and record them

        # logging.debug(f'Read {data.shape[0]} tweets; removing null entries')

        # bad_text = data['normalized'].isna()
        # data = data.drop(data[bad_text].index).reset_index(drop=True)
        #
        # logging.debug(f'Found {bad_text.sum()} bad text entries. Returning '
        #               f'remaining {data.shape[0]}.')

    return data


def save_csv(
        path: Path,
        df: DataFrame,
        name_scheme,
        batch=False,
        batch_size=1000,
        file_sep='~'):
    """
    Save a dataframe as a CSV; option to batch into multiple files

    :param path: location to save
    :param df: dataframe
    :param name_scheme: filename
    :param batch: batch the dataframe? (bool)
    :param batch_size: batch size if batch == True
    :param file_sep: CSV separator
    """
    try:
        if not batch:
            name = f'{name_scheme}-{df.shape[0]}'
            if '.csv' not in name_scheme:
                name = name + '.csv'

            df.to_csv(path / name, sep=file_sep, index=False)
            logging.info(f'Saved dataframe ({name_scheme}) CSV into: {path}')
        else:
            bins = ceil(df.shape[0] / batch_size)
            # Split into batches of approximately the specified batch size
            for i, b in enumerate(array_split(df, bins)):
                name = f'{name_scheme}-{i}-{b.shape[0]}.csv'
                b.to_csv(path / name, sep=file_sep, index=False)

            logging.info(f'Saved {bins} files into: {path}')
    except Exception as e:
        logging.exception(e.args)
        print(f'Problems saving data to CSV!')


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
            logging.info(f'Saved dataframe ({name_scheme}) xlsx into: {path}')
        else:
            bins = ceil(df.shape[0] / batch_size)
            # Split into batches of approximately the specified batch size
            for i, b in enumerate(array_split(df, bins)):
                name = f'{name_scheme}-{i}-{b.shape[0]}.xlsx'
                b.to_excel(path / name, index=False)

            logging.info(f'Saved {bins} files into: {path}')
    except Exception as e:
        logging.exception(e.args)
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
    Get datetime at time of this function's call. Format used is defined in
      the general config. Will return None if both date and time are false
    :param date: include date?
    :param time: include time?
    :return: datetime string representation
    """
    conf = get_config()

    fmt = ""
    if date and time:
        fmt = f'{conf["formats"]["date"]} {conf["formats"]["time"]}'
    elif date and (not time):
        fmt = f'{conf["formats"]["date"]}'
    elif (not date) and time:
        fmt = f'{conf["formats"]["time"]}'
    else:
        return None

    return datetime.now().strftime(fmt)


def get_logger(op,
               dt: datetime = datetime.now(),
               desc="",
               append=False):
    """
    Create and configure a logger with 3 handlers: console, detail, and progress
    Logs saved to .../lin-que-dropping/logs/

    :param op: operation being logged; used as part of log filename so keep short
    :param dt: (optional) datetime of log; must include the full time
      (year, month, day, hour, min, sec). Defaults to datetime.now()
    :param desc: (optional) readme description of log
    :param append: append to existing log if exists?
    :return: logger
    """

    try:
        conf = get_config('l')
        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)
        logger.propagate = False

        # Remove existing handlers (found they stuck between sessions)
        while logger.hasHandlers():
            logger.removeHandler(logger.handlers[0])

        # When no year specified, year defaults to 1900
        if dt.year==1900:
            raise ValueError(f'Passed datetime ({dt}) is missing a date')

        f = conf['formatters']['def']
        formatter = logging.Formatter(style=f['style'],
                                      fmt=f['format'],
                                      datefmt=f['datefmt'])

        # Console handler
        ch = conf['handlers']['console']
        console_hand = logging.StreamHandler()
        console_hand.setLevel(ch['level'])
        console_hand.setFormatter(formatter)

        # Logs partitioned by date; create directory (if doesn't exist)
        log_dir = get_str_datetime_now(True, False)
        log_path = get_project_root()/'logs'
        make_dir(log_path, log_dir)

        # Detail handler; records DEBUG and up
        dh = conf['handlers']['detail']
        detail_name = dh['filename'].format(dt.strftime("%H%M%S"), op)
        detail_hand = logging.FileHandler(filename=str(log_path/detail_name),
                                          mode='a' if append else 'w')
        detail_hand.setLevel(dh['level'])
        detail_hand.setFormatter(formatter)
        logger.addHandler(detail_hand)

        # Progress handler; records INFO and up
        ih = conf['handlers']['progress']
        prog_name = ih['filename'].format(dt.strftime("%H%M%S"), op)
        prog_hand = logging.FileHandler(filename=str(log_path/prog_name),
                                        mode='a' if append else 'w')
        prog_hand.setLevel(ih['level'])
        prog_hand.setFormatter(formatter)
        logger.addHandler(prog_hand)

        # Update log README
        save_readme(log_path, f'{detail_name}: {desc}')

        return logger
    except Exception as e:
        print(f'Error getting logger! \n{e.args}')
        return None


def get_config(conf_type='g',
               path: Path = None) -> dict:
    """
    Return a YAML configuration dict.
    :param conf_type: choice of 'g, l, e, c, p'
      for 'general, log, extraction, cleaning, processing' configurations files
      (default: 'g')
    :param path: (optional) path to config
    """
    types = {'g': 'general',
             'conn': 'connection',
             'l': 'log',
             'e': 'extraction',
             'c': 'cleaning',
             'p': 'processing'}

    try:
        if path is not None:
            with open(path, 'r') as f:
                logging.info(f'Opened config file at: {path}')
                return yaml.safe_load(f)

        config_path = get_project_root()/'config'/f'{types[conf_type]}_config.yml'

        with open(config_path, 'r') as f:
            logging.info(f'Opened config file at: {config_path}')
            return yaml.safe_load(f)

    except Exception as e:
        logging.exception(f'Failed to open config file! {e.args}')


def get_updated_config(conf: dict, path: Path) -> dict:
    """
    Update and return a config file
    """
    try:
        with open(path, 'w') as f:
            yaml.dump(conf, f)

        return get_config(path=path)

    except Exception as e:
        logging.exception(f'Failed to update config file!\n >>> {e.args} <<<')
        return conf
    finally:
        logging.info(f'Updated config file at: {path}')


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

    logging.info(f'Removed empty directories in {str(path)}: \n{removed}')
    print(removed)