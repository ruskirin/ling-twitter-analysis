import pandas as pd
import json
import importlib.util as imp
import sys
import os

spec = imp.spec_from_file_location(
    'twitter_data',
    '../twitter-connection/twitter_data/__init__.py')
twit = imp.module_from_spec(spec)
sys.modules[spec.name] = twit
spec.loader.exec_module(twit)

from twitter_data import tweets, users, places


'''
Module responsible for the GET responses made in connection.py
'''
class Response:
    def __init__(self, lang, topic, response = None):
        self.lang = lang
        self.topic = topic

        self.schema = dict()
        if response is not None:
            self.generate_tables(response)

        self.pulls = 0

    # Recursive method to expand @response and fill @self.tables with DataFrames
    def generate_tables(self, response):
        for table in response.items():
            if table[0]=='meta':
                self.schema['meta'] = table[1]
                continue
            elif table[0]=='data':
                self.schema['data'] = tweets.Tweets(table[1])
                continue
            elif table[0]=='users':
                self.schema['users'] = users.Users(table[1])
                continue
            elif table[0]=='places':
                self.schema['places'] = places.Places(table[1])
                continue

            if not isinstance(table[1], dict):
                # If not a dictionary but a list, add as DataFrame.
                #   Else add as a regular entry
                self.schema[table[0]] = pd.json_normalize(table[1]) \
                    if isinstance(table[1], list) \
                    else table[1]
                continue

            # Recursive call to expand subtable
            self.generate_tables(table[1])

    def append(self, response):
        if len(self.schema)==0:
            self.schema = response.schema
            return

        print(f'Before append: {self.schema["data"].original.shape[0]}')

        for table in response.schema.items():
            if table[0]=='meta':
                continue

            if table[0] not in self.schema:
                self.schema[table[0]] = table[1]
                continue

            self.schema[table[0]].append(table[1])

        print(f'After append: {self.schema["data"].original.shape[0]}')

    def join(self, to, data, on, how='left'):
        try:
            self.schema[to] = self.schema[to].merge(data, on=on, how=how)
        except:
            print('Some columns failed to merge')

    def reset_index(self):
        if len(self.schema)==0:
            return

        for table in self.schema.items():
            if table[0]=='meta':
                continue

            table[1].reset_index(drop=True, inplace=True)

    def save_csv(self, time, is_test=False):
        make_dir(time, self.lang, is_test)
        prefix = f'{"test" if is_test else "saved"}/{self.lang}/{time}'

        try:
            for table in self.schema.items():
                if table[0]=='meta':
                    continue

                if isinstance(table[1], tweets.twitter_data.TwitterData):
                    table[1].save_csv(
                        prefix, lang=self.lang, topic=self.topic, num=self.pulls)

        except Exception as e:
            print(f'Exception while saving to CSV! '
                  f'>>>\n {e.args} \n<<<')
        finally:
            self.pulls+=1


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


def make_dir(time, lang, is_test=False):
    try:
        path = f'./{"test" if is_test else "saved"}'
        os.mkdir(f'{path}/{lang}/{time}')
    except FileExistsError as e:
        return
    except Exception as e:
        print(f'Exception raised while making dir! '
              f'>>>\n {e.args[0]} \n<<<')