import os
import logging
import regex
import pandas as pd
from pathlib import Path
from unidecode import unidecode


logger = logging.getLogger(__name__)


# Module-wide
# TODO LIST:
#   - test update_ids()
#   -
#   -
#   -


# def split_


def update_ids(dir: Path, id_col: str, data_type: str) -> (int, set):
    """
    Extract all unique @id entries from all CSV files in @dir and append to
      respective <@data_type>.txt file in ../lin-que-dropping/data/ids/ .
      Returns a set of duplicated ids which already existed in the text file.

      NOTE: does not recognize all duplicates; run a dedicated duplicate finder
        method to ensure unique tokens! The method of identifying duplicates
        does not look for them in the passed files themselves -- eg. some file
        A in @dir has x > 1 instances of token id m, m is new to the stored
        records and will thus be considered unique, and all x entries will
        bypass the duplicate checker!!!

    :param dir:
    :param id_col:
    :param data_type: one of {'tweets', 'users', 'places', 'twitterdata'}
    :return: tuple (number new entries, set of duplicated ids (if any))
    """
    if data_type not in {'tweets', 'users', 'places', 'twitterdata'}:
        return None

    conf = u.get_config()
    id_path = u.get_project_root()/conf['file_paths']['ids']/(data_type + '.txt')

    with open(id_path, 'r') as f:
        ids = {i.strip() for i in f.read().split(',')}

    na_ids = len(ids) # total number ids before update
    logger.debug(f'Updating "{data_type}" ids; {na_ids} existing')

    duplicates = set() # keep track of duplicate entries
    for f in dir.iterdir():
        # Series -> unique -> set is significantly faster than Series -> set
        data = pd.read_csv(f, sep='~', usecols=[id_col])[id_col].astype(str)
        fids = set(data.unique())
        dupes = ids.intersection(fids) # identify duplicates
        duplicates.update(dupes)

        ids.update(fids)

    nb_ids = len(ids) # total number of ids after update
    logger.debug(f'{nb_ids} (+{nb_ids-na_ids}) unique ids after update. '
                 f'\nFound {len(duplicates)} duplicate ids.')

    with open(id_path, 'w') as f:
        f.write(','.join(ids))

    return nb_ids-na_ids, duplicates



def remove_dups_extracted(e_path):
    pass


def folder_dup_clean(
        cleaned: set,
        path: Path,
        file_identifier,
        dup_subset,
        file_csv_sep='~',
        delete_original=True,
        batch=False,
        batch_size=1000,
        name_scheme=''):

    """
    Find and remove all duplicate entries from CSV files in specified folder.
    Targeted files must have a standardized name schema

    :param cleaned: already processed/cleaned folders
    :param path: folder path as a Path object
    :param file_identifier: common name pattern found in desired files
    :param dup_subset: column name by which to identify duplicates
    :param file_csv_sep: CSV separator used
      (default: '~')
    :param delete_original: whether to delete the cleaned files
      (default: True)
    :param batch: whether to separate cleaned dataframe into multiple CSV files
      (default: False)
    :param batch_size: if batch=False, batch size for separate files
    :param name_scheme: cleaned CSV file naming scheme, will default to
      @file_identifier otherwise

    :return: tuple (original tweets, duplicates removed)
    """
    logging.info(f'Checking for duplicates in: {path}\n'
                 f'  Identifying duplicates by: {dup_subset}\n'
                 f'  Deleting original: {delete_original}\n'
                 f'  Name scheme: {name_scheme}')

    original_total = 0
    dup_total = 0

    for folder in path.iterdir():
        if folder in cleaned:
            logging.debug(f'{folder} has been cleaned')
            continue

        name_pat = fr'(?<!cleaned.+){file_identifier}'

        logging.debug(f'Cleaning ({file_identifier}) in: {folder}')
        # Identify all files containing @file_identifier
        paths = [f for f in os.listdir(folder)
                 if regex.search(name_pat, f) is not None]
        # Folder already cleaned or no files with @file_identifier exist
        if len(paths)==0:
            logging.debug(f'({file_identifier}) missing from folder: {folder}')
            continue

        # Concatenate all matched files. Reset index
        matched = pd.concat(
            [pd.read_csv(
                path/folder/p,
                sep=file_csv_sep,
                lineterminator='\n') for p in paths]
        ).reset_index(drop=True)

        original_total += matched.shape[0]
        logging.debug(f'Total {matched.shape[0]} tweets')

        dup = matched.duplicated(subset=dup_subset)
        dup_total += dup.sum()
        matched.drop(matched[dup].index, axis=0, inplace=True)
        logging.debug(f'Dropped {dup.sum()} {file_identifier} duplicates in {folder}')

        name = f'{folder}-cleaned-{name_scheme if name_scheme!="" else file_identifier}'
        u.save_csv(path/folder,
                       matched,
                       name,
                       batch,
                       batch_size,
                       file_csv_sep)

        if delete_original:
            for p in paths:
                os.remove(path/folder/p)

        cleaned.add(folder)

    return original_total, dup_total


def combine_cols(tweet, cols:list) -> tuple:
    return tuple(tweet.loc[cols].values)


def standardize_col_name(col):
    """
    Standardize column names; primarily used in Corpes data
    """
    return unidecode(col).lower()


def assign_ids(df: pd.DataFrame, start=0) -> pd.DataFrame:
    """
    Assign row ids to all data files in path; ids are assigned consecutively as
      ascending integer values

    :param df: dataframe
    :param start: id to give the first entry
    :return: id of final entry
    """
    data = df.reset_index()
    data = data.rename({'index': 'id'}, axis=1)
    data['id'] = data['id'] + start

    return data


def separate_by_verb():
    """

    """
    pass


if __name__ == '__main__':
    n, dupes = update_ids(data_type='tweets',
                          dir=u.choose_save_path('e', '2023-02-08-at-23:05:39/tweets/'),
                          id_col='tweet_id')
    print(n, len(dupes))