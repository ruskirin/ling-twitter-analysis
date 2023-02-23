import yaml
from files import get_project_root
from pathlib import Path
from logging import getLogger


logger = getLogger(__name__)

types = {'g': 'general',
         'conn': 'connection',
         'l': 'log',
         'e': 'extraction',
         'c': 'cleaning',
         'p': 'processing'}


def get_yaml(path: Path):
    """Return the contents of a yaml file"""
    try:
        with open(path, 'r') as f:
            logger.info(f'Opened config file: {path.stem}')
            return yaml.safe_load(f)

    except TypeError as e:
        logger.exception(f'Failed to open config file!\n{e.args}')
        raise


def read_conf(conf_type='g') -> dict | None:
    """
    Return a YAML configuration dict.
    :param conf_type: choice of 'g, l, e, c, p'
      for 'general, log, extraction, cleaning, processing' configurations files
      (default: 'g')
    """

    if conf_type not in types:
        raise ValueError(f'Must pass valid @conf_type; one of: \n{types.items()}')

    try:
        config_path = get_project_root()/'config'/f'{types[conf_type]}_config.yml'
        return get_yaml(config_path)
    except Exception as e:
        logger.exception(f'Failed to open config file! {e.args}')
        raise


def update_conf(conf: dict, conf_type: str) -> dict:
    """
    Update a config file and return it

    :param conf: configuration file as a dictionary
    :param conf_type: one of {g, l, conn, e, c, p} for 'general, log,
      connection, extraction, cleaning, processing' configurations files
    :return: configuration file dictionary
    """
    try:
        path = get_project_root()/'config'/f'{types[conf_type]}_config.yml'
        with open(path, 'w') as f:
            yaml.dump(conf, f)

        return read_conf(conf_type)

    except Exception as e:
        logger.exception(f'Failed to update config file!\n{e.args}')
        return conf
    finally:
        logger.info(f'Updated config file at: {conf_type}')