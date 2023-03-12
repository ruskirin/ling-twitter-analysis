from pathlib import Path
from logging import getLogger
from datetime import datetime
import pandas as pd
import regex as re
import configs


logger = getLogger(__name__)


def get_str_datetime_now(date: bool = True, time: bool = True):
    """
    Get datetime at time of this function's call; used for directory and file
    naming. Format used is defined in the general config. Will return None if
    both date and time are false

    :param date: include date?
    :param time: include time?
    :return: datetime string representation
    """
    conf = configs.read_conf()

    if date and time:
        fmt = f'{conf["formats"]["date"]}-at-{conf["formats"]["time"]}'
    elif date and (not time):
        fmt = f'{conf["formats"]["date"]}'
    elif (not date) and time:
        fmt = f'{conf["formats"]["time"]}'
    else:
        return None

    return datetime.now().strftime(fmt)


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


def get_save_path(loc: str,
                  corpus: str = 'twitter',
                  is_test: bool = False,
                  lang: str = 'es') -> Path:
    """
    Get filepath to the saved data

    :param loc: 'e' extracted, 'c' cleaned, 'p' processed
    :param corpus: 'twitter' or 'corpes'
    :param is_test: get the test saves
    :param lang: language of tweets (separated by language); default is just the saved
      folder
    """
    dirs = {'e': 'extracted', 'c': 'cleaned', 'p': 'processed'}
    corpuses = {'twitter', 'corpes'}

    try:
        if loc not in dirs.keys():
            raise ValueError(f'@loc "{loc}" is not in one of ({list(dirs.keys())})')
        if corpus not in corpuses:
            raise ValueError(f'@corpus "{corpus}" is not one of ({corpuses})')

        data_path = get_project_root()\
                    / 'data'\
                    / dirs[loc]\
                    / ('saved' if not is_test else 'test')\
                    / corpus\
                    / lang

        return data_path

    except ValueError as e:
        logger.exception(e.args)
        raise


def choose_save_path(loc,
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
    dir = get_save_path(loc, corpus, is_test, lang)
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


def get_verb_conjugations() -> pd.DataFrame:
    conf = configs.read_conf('g')

    p = get_project_root()/conf['file_paths']['verb_conjug']
    return pd.read_excel(p)


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