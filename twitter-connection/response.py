import pandas as pd
import json

'''
Module responsible for the GET responses made in connection.py
'''
class Response:
    def __init__(self, response: dict, include: list):
        self.schema = response.keys()

        self.data = pd.json_normalize(response['data'])
        # TODO: custom setters/getters
        self.users = dict()
        # TODO: custom setters/getters
        self.places = dict()

    def get_df(self, *names):
        dfs = dict()

        for name in names:
            if name not in self.schema:
                print(f'No key "{name}" in table! Check @schema')
                continue

            dfs[name] = self.response[name]

        return dfs

    # def save_


def save(path, data):
    with open(path, 'w') as f:
        try:
            if isinstance(data, dict):
                json.dump(data, f)
            elif isinstance(data, str):
                f.write(json.dumps(data))
        except:
            f'Exception raised while saving into: {path}'


def retrieve(path):
    with open(path, 'r') as f:
        return json.load(f)