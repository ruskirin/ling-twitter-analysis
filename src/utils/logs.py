from datetime import datetime
import logging
import pandas
import configs
import files


logger = logging.getLogger(__name__)

def save_readme(location, text, append=True):
    op = 'a+' if append else 'w'

    with open(location/'README.txt', op) as readme:
        readme.seek(0)

        if append and (len(readme.read(100)) > 0):
            text = '\n\n' + text

        readme.write(text)


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
        dir_name = files.get_str_datetime_now(True, False)
        log_dir_path = files.get_project_root() / 'logs'
        log_path = files.make_dir(log_dir_path, dir_name)

        conf = configs.read_conf('l')
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