import logging
from datetime import datetime
import pandas as pd
from pathlib import Path
import importlib
import sys

spec_src = importlib.util.spec_from_file_location(
    'src',
    '../../__init__.py')
m = importlib.util.module_from_spec(spec_src)
sys.modules[spec_src.name] = m
spec_src.loader.exec_module(m)

from src import utils


# def merge_batches()


def save_by_verb(df: pd.DataFrame,
                 save_from: str,
                 verb_conjug_path: Path,
                 save_path: Path,
                 save_file_type: str = 'csv',
                 batch_size=-1) -> bool:
    # TODO: hardcode verb_conjug_path as it is specific to proejct
    # TODO: add option for language

    """
    Partition dataframe by verbs and save in separate directories accordingly

    :param df: dataframe
    :param save_from: one of 'twitter' or 'corpes' (used during file naming)
    :param verb_conjug_path: path to excel file with verb conjugations
    :param save_path: location to save data
    :param save_file_type: format to save data in -- one of 'csv' or 'excel'
    :param batch_size: (optional) specify batch size to save in batches
    :return: save successful (bool)
    """
    if save_from not in ['twitter', 'corpes']:
        raise ValueError('Invalid input for save_from; must be one of '
                         '["twitter", "corpes"]')
    if save_file_type not in ['csv', 'excel']:
        raise ValueError('Invalid input for save_file_type; must be one of '
                         '["csv", "excel"]')

    # Used in filename
    # time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    logging.info(f'Starting save of {df.shape[0]} entries into {save_path}')

    df['verbs'] = df['verbs'].str.split(', ')

    conjugs = pd.read_excel(verb_conjug_path)
    verbs = set(conjugs['verb'].to_numpy())

    logging.debug(f'Separating by verbs: \n{verbs}')

    for verb in verbs:
        vtype = conjugs.loc[conjugs['verb']==verb, 'verb_type'].iloc[0].lower()
        has_verb = df['verbs'].apply(lambda vs: True if verb in set(vs) else False)

        verb_df = df[has_verb].copy()

        logging.debug(f'Saving {verb_df.shape[0]} entries of ({verb})')

        utils.make_dir(save_path, vtype)

        path = save_path/vtype

        if save_file_type=='csv':
            utils.save_csv(
                path,
                verb_df,
                f'{save_from}-es-{verb}',
                batch=True if batch_size != -1 else False,
                batch_size=batch_size
            )
        else:
            utils.save_excel(
                path,
                verb_df,
                f'{save_from}-es-{verb}',
                batch=True if batch_size != -1 else False,
                batch_size=batch_size
            )