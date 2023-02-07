import json
import logging
from twitter_data import twitter_data, tweets, users, places


class Response:
    """
    Module responsible for the GET responses made in connection.py
    """
    def __init__(self, lang, topic, response = None):
        self.lang = lang
        self.topic = topic

        self.schema = dict()
        if response is not None:
            self.generate_tables(response)

    # Recursive method to expand @response and fill @self.tables with DataFrames
    def generate_tables(self, response):
        logging.debug(f'Generating tables from keys: {response.keys()}')

        for table in response.items():
            if table[0]=='meta':
                self.schema['meta'] = table[1]
                continue
            elif table[0]=='data':
                self.schema['data'] = tweets.Tweets.from_json(table[1])
                continue
            elif table[0]=='users':
                self.schema['users'] = users.Users.from_json(table[1])
                continue
            elif table[0]=='places':
                self.schema['places'] = places.Places.from_json(table[1])
                continue

            if not isinstance(table[1], dict):
                # If not a dictionary but a list, add as DataFrame.
                #   Else add as a regular entry
                self.schema[table[0]] = twitter_data.TwitterData.from_json(table[1])\
                    if isinstance(table[1], list) \
                    else table[1]
                continue

            # Recursive call to expand subtable
            self.generate_tables(table[1])

    def get_next_token(self):
        try:
            return self.schema['meta']['next_token']
        except KeyError:
            logging.debug(f'Returned metadata:\n{self.schema["meta"]}')
            logging.info('No next token to paginate')
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

        logging.debug(f'After append: {self.schema["data"].data.shape[0]}')

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

                if isinstance(table[1], tweets.twitter_data.TwitterData):
                    table[1].save_csv(
                        folder_path, lang=self.lang, topic=self.topic, num=pulls)

        except Exception as e:
            logging.exception(f'Exception while saving to CSV! {e.args}')


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


