import requests
import logging
from logging import config
import yaml
from time import sleep
import response as tr
import importlib
import importlib.util as imp
import sys

spec_src = imp.spec_from_file_location(
    'src',
    '../__init__.py')
m = imp.module_from_spec(spec_src)
sys.modules[spec_src.name] = m
spec_src.loader.exec_module(m)

from src import utils


class TwitterConnection:
    """
    :param lang: specify language of connection
    :param bearer_path: key name for desired token to be found in credentials yaml;
      use '/' separation for nested entries
    :param is_archive: is this a 'Full-Archive Search'? If not, then is 'Recent Search'
    """
    def __init__(self,
                 lang: str,
                 bearer_path: str,
                 is_archive: bool = False):

        self.lang = lang
        self.bearer_path = bearer_path
        self.is_archive: bool = is_archive

        self.gen_conf = utils.get_config()
        self.conf = utils.get_config('conn')
        self.credentials = dict()

        try:
            with open(utils.get_project_root()/
                      self.gen_conf["file_paths"]["credentials"], 'r') as cred_file:
                self.credentials = yaml.safe_load(cred_file)

        except Exception as e:
            logging.exception(f'Exception reading in .yml: '
                                  f'\n{e.args}')

        self.header = self.create_headers()

        # Saved response from .connect()
        self.response = None
        # Extracted next token for pagination of searches
        self.next_token = ''
        self.has_next = False

        self.pages = 0

    # If raw token was passed during object creation, use that
    #   else look for a token in @cred_path
    def auth(self):
        try:
            assert len(self.credentials)>0

            token = None
            for i, path in enumerate(self.bearer_path.split('/')):
                if i==0:
                    token = self.credentials[path]
                else:
                    token = token[path]

            logging.debug(f'Authorizing connection, token: {token}')

            return token

        except AssertionError:
            logging.exception('Failed to read in credentials')

    # Get proper format for bearer token
    def create_headers(self):
        return {'Authorization': f'Bearer {self.auth()}'}

    # Combine query, fields and (if available) next_token into a proper URL
    def create_url(self, topic, next_token=None):
        prefix = self.conf['paths']['query_search']\
            ['prefix_archive' if self.is_archive else 'prefix_recent']
        fields = self.conf['query_fields']

        if (next_token is not None) and (len(next_token)>0):
            return f'{prefix}={topic}' + ' ' + \
                       f'lang:{self.lang} {fields["conditions"]}' \
                       f'&{fields["max_results"]}' \
                       f'&next_token={next_token}' \
                       f'&{fields["tweet"]}' \
                       f'&{fields["expansions"]}' \
                       f'&{fields["user"]}' \
                       f'&{fields["place"]}'
        else:
            return f'{prefix}={topic}' + ' ' + \
                       f'lang:{self.lang} {fields["conditions"]}' \
                       f'&{fields["max_results"]}' \
                       f'&{fields["tweet"]}' \
                       f'&{fields["expansions"]}' \
                       f'&{fields["user"]}' \
                       f'&{fields["place"]}'

    def paginate(self,
                 save_path,
                 topic: tuple,
                 batch_size=1000,
                 batch_num=1,
                 sleep_sec=0):

        """
        Make a series of .connect() calls until the desired @batch_size is reached
          or no more results available

        :param save_path: location to save extracted tweets
        :param topic: (name, topic) pair
          save will include name in filename
          topic is used for the query
        :param batch_size: tweets extracted between saves
        :param batch_num: amount of batches to extract
        :param sleep_sec: time (seconds) between consecutive queries
        """

        logging.info(f'Starting pagination of: {topic[0]}'
                     f'\nPagination save path: {save_path}')

        self.pages = 0

        data = self.connect(topic)
        logging.debug(f'Initial connection established.')

        while self.has_next and (self.pages < batch_num):
            sleep(sleep_sec)

            next_token = self.get_next_token()
            if not self.has_next:
                continue

            data.append(self.connect(topic, next_token))

            if (data.schema['data'].data.shape[0] >= batch_size) \
                    and self.has_next:
                logging.debug(f'Saving batch.')

                data.save_csv(self.pages, save_path)
                self.pages+=1

                logging.info(f'\tRetrieved {self.pages} pages')

                data = tr.Response(self.lang, topic[0])

        logging.info(f'\tPagination finished; retrieved {self.pages*batch_size} total pages')

        data.save_csv(self.pages, save_path)
        return self.pages

    def connect(self, query_topic, next_token=None):
        """
        Make a single connection and save response

        :param query_topic: just the TOPIC of your query -- fields and "rules" have already
          been set through .set_fields()
        :param next_token: token for next page
        """
        self.has_next = True

        url = self.create_url(query_topic[1], next_token)
        logging.debug(f'URL: {url}')

        try:
            response = self._connect_to_endpoint(url, self.header)

            self.response = tr.Response(self.lang,
                                        query_topic[0],
                                        response.json())
            return self.response

        except ConnectionError as ce:
            if ce.args[0].status_code==429:
                logging.exception(
                    'Too many requests! Pausing for 5 seconds...')
                sleep(5)

                return self.connect(query_topic, next_token)
            else:
                logging.exception(f'{ce.args[0].status_code}\n'
                                      f'{ce.args[0].text}')

        except AttributeError as ae:
            logging.exception(f'Problem reading response attributes!'
                                  f'\n{ae.args}')

    def get_next_token(self):
        try:
            assert self.response is not None
        except AssertionError:
            logging.exception(
                'self.response is None; did you try paginating without '
                'making the initial connection?')

        next_token = self.response.get_next_token()
        logging.debug(f'Next token: {next_token}')

        if self.next_token==next_token:
            logging.exception(f'Same pagination token returned')

        if next_token is None:
            self.has_next = False

        self.next_token = next_token

        return next_token

    # Makes a get request and returns the response
    def _connect_to_endpoint(self, url, headers):
        response = requests.request('GET', url, headers=headers)

        if response.status_code != 200:
            raise ConnectionError(response)

        return response