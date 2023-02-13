import os
import logging
import regex
import pandas as pd
from pathlib import Path
from unidecode import unidecode
import utils as u


logger = logging.getLogger(__name__)


# def split_


def update_ids(dir: Path, id_col: str, data_type: str) -> list|None:
    """
    TODO: CONTINUE WORKING ON KEEPING TRACK OF DUPLICATE ENTRIES


    Extract all unique @id entries from all CSV files in @dir and append to
      respective <@data_type>.txt file in ../lin-que-dropping/data/ids/

    :param dir:
    :param id_col:
    :param data_type: one of {'tweets', 'users', 'places', 'twitterdata'}
    :return: list of duplicated entries (if any)
    """
    if data_type not in {'tweets', 'users', 'places', 'twitterdata'}:
        return None

    conf = u.get_config()
    id_path = u.get_project_root()/conf['file_paths']['ids']/(data_type + '.txt')

    with open(id_path, 'r') as f:
        ids = {i.strip() for i in f.read().split(',')}

    duplicates = set() # keep track of duplicate entries
    for f in dir.iterdir():
        data = u.get_csv(f, columns=['tweet_id']).to_numpy()

    return ids



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
    print(update_ids(None, None, data_type='tweets'))