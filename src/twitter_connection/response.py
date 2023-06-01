import json
from logging import getLogger
from pathlib import Path
from twitter_data import twitter_data, tweets, users, places


logger = getLogger(__name__)


class Response:
    """
    Object representing twitter api's query response

    :param lang:
    :param topic:
    :param response:
    """
    def __init__(self, lang, topic, response):
        self.lang = lang
        self.topic = topic
        # dict of various data extracted from query
        self.tables = dict()
        self.extract_data(response)

    @property
    def next_token(self):
        """Token used to navigate to next page of query"""
        try:
            return self.tables['meta']['next_token']
        except KeyError:
            logger.info('No next token to paginate')
            return None

    def __len__(self):
        try:
            return self.tables['data'].shape[0]
        except (KeyError, TypeError) as e:
            logger.exception(e.args)
            return None

    def extract_data(self, response):
        """
        Recursive method to expand @response and fill @self.schema with
          DataFrames. Check the data type returned in response and assign
          the appropriate DataFrame subclass.

        :param response: Twitter query response object
        :return:
        """
        logger.debug(f'Generating tables from keys: {response.keys()}')

        for t_type, table in response.items():
            if t_type=='meta':
                self.tables[t_type] = table
            elif t_type=='data':
                data = tweets.Tweets.from_json(table, self.topic, self.lang)
                self.tables['data'] = data
            elif t_type=='users':
                data = users.Users.from_json(table, self.topic, self.lang)
                self.tables[t_type] = data
            elif t_type=='places':
                data = places.Places.from_json(table, self.topic, self.lang)
                self.tables[t_type] = data
            elif isinstance(table, list):
                # table is a list of values
                data = twitter_data.TwitterData.from_json(table, self.topic, self.lang)
                self.tables[t_type] = data
            elif not isinstance(table, dict):
                # table is some other object; save it
                self.tables[t_type] = table
            else:
                # Recursive call to expand subtable
                self.extract_data(table)

    def append(self, response):
        if len(self.tables)==0:
            # this instance is empty, initialize
            self.tables = response.tables
            return

        for t_type, table in response.tables.items():
            # TODO 2/22: see if overwriting all the previous metadata is
            #   the right move in this context

            # get the metadata from the latest response
            if (t_type == 'meta') or (t_type not in self.tables):
                self.tables[t_type] = table
            else:
                self.tables[t_type].append(table)

        logger.debug(f'After append: {self.tables["data"].d.shape[0]}')

    def reset_index(self):
        if len(self.tables)==0:
            return

        for table in self.tables.items():
            if table[0]=='meta':
                continue

            table[1].reset_index(drop=True, inplace=True)

    def save_csv(self, path: Path, batch=None):
        """Save extracted data as CSV"""
        try:
            for t_type, data in self.tables.items():
                # TODO 2/22: see if metadata should be saved instead
                if t_type=='meta':
                    continue

                if isinstance(data, twitter_data.TwitterData):
                    data.save(path, batch_num=batch, sep_by_type=True)

        except Exception as e:
            logger.exception(e.args)
            raise


def save_json(path, data):
    with open(path, 'w') as f:
        try:
            if isinstance(data, dict):
                json.dump(data, f)
            elif isinstance(data, str):
                f.write(json.dumps(data))
        except:
            print(f'Exception raised while saving into: {path}')


def retrieve(path):
    with open(path, 'r') as f:
        return json.load(f)


