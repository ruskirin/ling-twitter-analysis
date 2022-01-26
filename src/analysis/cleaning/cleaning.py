import os
import logging
import regex
import pandas as pd
from pathlib import Path
from unidecode import unidecode
import importlib.util as imp
import sys

spec_src = imp.spec_from_file_location(
    'src',
    '../../__init__.py')
m = imp.module_from_spec(spec_src)
sys.modules[spec_src.name] = m
spec_src.loader.exec_module(m)

from src import utils


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
    Targeted filed must have a standardized name schema.

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
            [pd.read_csv(path/folder/p, sep=file_csv_sep, lineterminator='\n') for p in paths])\
              .reset_index(drop=True)
        original_total += matched.shape[0]
        logging.debug(f'Total {matched.shape[0]} tweets')

        dup = matched.duplicated(subset=dup_subset)
        dup_total += dup.sum()
        matched.drop(matched[dup].index, axis=0, inplace=True)
        logging.debug(f'Dropped {dup.sum()} {file_identifier} duplicates in {folder}')

        name = f'{folder}-cleaned-{name_scheme if name_scheme!="" else file_identifier}'
        utils.save_csv(path/folder,
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


# def standardize()


# def remove_dups(
#         dfs: list,
#         dup_id,
#         dfs_directory=None,
#         csv_sep='~',
#         return_distrib=False):
#
#     dfs_type = type(dfs[0])
#
#     if ((dfs_type is str) or (dfs_type is Path)) and  dfs_directory is not None:
#         dfs = [pd.read_csv(dfs_directory/p, sep=csv_sep) for p in dfs]
#
#     assert type(dfs[0]) is pd.DataFrame, "Couldn't read passed @dfs data; " \
#                                          "make sure is list of DataFrames or " \
#                                          "CSV paths"
#
#     if not return_distrib:
#         dup = matched.duplicated(subset=dup_subset)
#         matched.drop(matched[dup].index, axis=0, inplace=True)