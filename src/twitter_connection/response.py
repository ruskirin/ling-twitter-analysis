import json
from logging import getLogger
from twitter_data import TwitterData, Tweets, Users, Places


logger = getLogger(__name__)


class Response:
    """
    Object representing twitter api's query response

    :param lang:
    :param topic:
    :param response:
    """
    def __init__(self, lang, topic, response = None):
        self.lang = lang
        self.topic = topic
        # dict of various data extracted from query
        self.schema = dict()
        if response is not None:
            self.generate_tables(response)

    def generate_tables(self, response):
        """
        Recursive method to expand @response and fill @self.tables with
          DataFrames. Check the data type returned in response and assign
          the appropriate DataFrame subclass.

        :param response: Twitter query response object
        :return:
        """
        logger.debug(f'Generating tables from keys: {response.keys()}')

        for table in response.items():
            t_type = table[0]

            if t_type=='meta':
                self.schema[t_type] = table[1]
            elif t_type=='data':
                data = Tweets.from_json(table[1], self.topic, self.lang)
                self.schema['data'] = data
            elif t_type=='users':
                data = Users.from_json(table[1], self.topic, self.lang)
                self.schema[t_type] = data
            elif t_type=='places':
                data = Places.from_json(table[1], self.topic, self.lang)
                self.schema[t_type] = data
            elif isinstance(table[1], list):
                data = TwitterData.from_json(table[1], self.topic, self.lang)
                self.schema[t_type] = data
            elif not isinstance(table[1], dict):
                # If not a dictionary but a list, add as DataFrame.
                #   Else add as a regular entry
                self.schema[t_type] = table[1]

            # Recursive call to expand subtable
            self.generate_tables(table[1])

    def get_next_token(self):
        try:
            return self.schema['meta']['next_token']
        except KeyError:
            logger.debug(f'Returned metadata:\n{self.schema["meta"]}')
            logger.info('No next token to paginate')
            return None

    def append(self, response):
        if len(self.schema)==0:
            self.schema = response.schema
            return

        for table in response.schema.items():
            if table[0]=='meta':
                continue

            if table[0] not in self.schema:
                self.schema[table[0]] = table[1]
                continue

            self.schema[table[0]].append(table[1])

        logger.debug(f'After append: {self.schema["data"].data.shape[0]}')

    def reset_index(self):
        if len(self.schema)==0:
            return

        for table in self.schema.items():
            if table[0]=='meta':
                continue

            table[1].reset_index(drop=True, inplace=True)

    def save_csv(self, pulls, folder_path):
        """
        Save Response as a csv

        @time, @pulls, @is_test: used during folder creation for save
        @path: alternatively, supply full folder name to save in
        """
        try:
            for table in self.schema.items():
                if table[0]=='meta':
                    continue

                if isinstance(table[1], TwitterData):
                    table[1].save_csv(
                        folder_path, lang=self.lang, topic=self.topic, num=pulls)

        except Exception as e:
            logger.exception(f'Exception while saving to CSV! {e.args}')


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


