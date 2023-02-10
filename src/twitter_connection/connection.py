import requests
from logging import getLogger
from decouple import config, UndefinedValueError
from time import sleep
from response import Response
from utils import get_config


logger = getLogger(__name__)


class TwitterConnection:
    """
    Single connection to the twitter api

    :param lang: specify language of connection
    :param username: name of API key; formatted as
      "<uppercase_username>_KEY: <key>" in an environ
    :param is_archive: is this a 'Full-Archive Search'? If not, then is 'Recent Search'
    """
    def __init__(self,
                 lang: str,
                 is_archive: bool = False,
                 key: str = None,
                 key_name: str = None):

        self.lang = lang
        self.is_archive = is_archive

        self.conf = get_config('conn')

        self.header = self.create_headers(key, key_name)

        # Saved response from .connect()
        self.response = None
        # Extracted next token for pagination of searches
        self.next_token = ''
        self.has_next = False

        self.pages = 0

    def create_headers(self, key=None, env_key_name=None):
        """Format the bearer token as requested"""
        if env_key_name is not None:
            env_key_name = env_key_name.upper() + '_KEY'

        return {'Authorization': f'Bearer {self._auth(key, env_key_name)}'}

    def create_url(self, topic, next_token=None):
        """
        Combine query, fields and (if available) next_token into a proper URL
        """
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

        # TODO: THE SAVING OF FILES IS DONE RETROACTIVELY -- NEW PULLS
        #   ARE GETTING SAVED INTO PREVIOUSLY NAMED FILES, SO YOU GET MISMATCHED
        #   FILENAMES AND PULLS

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

        logger.info(f'Starting pagination of: {topic[0]}'
                     f'\nPagination save path: {save_path}')

        self.pages = 0

        data = self.connect(topic)
        logger.debug(f'Initial connection established.')

        while self.has_next and (self.pages < batch_num):
            sleep(sleep_sec)

            next_token = self.get_next_token()
            if not self.has_next:
                continue

            data.append(self.connect(topic, next_token))

            if (data.schema['data'].data.shape[0] >= batch_size) \
                    and self.has_next:
                logger.debug(f'Saving batch.')

                data.save_csv(self.pages, save_path)
                self.pages+=1

                logger.info(f'\tRetrieved {self.pages} pages')

                data = Response(self.lang, topic[0])

        logger.info(f'\tPagination finished; retrieved {self.pages*batch_size} total pages')

        # Save last extracted batch
        data.save_csv(self.pages, save_path)
        return self.pages

    def connect(self, query_topic, next_token=None):
        """
        Make a single connection and save response

        :param query_topic: just the TOPIC of your query -- fields and "rules" have already
          been set through .set_fields()
        :param next_token: token for next page
        :return: response.Response object
        """
        self.has_next = True

        url = self.create_url(query_topic[1], next_token)
        logger.debug(f'URL: {url}')

        try:
            response = self._connect_to_endpoint(url, self.header)

            self.response = Response(
                self.lang, query_topic[0], response.json()
            )
            return self.response

        except ConnectionError as ce:
            if ce.args[0].status_code==429:
                logger.exception(
                    'Too many requests! Pausing for 5 seconds...')
                sleep(5)

                return self.connect(query_topic, next_token)
            else:
                logger.exception(f'{ce.args[0].status_code}\n'
                                      f'{ce.args[0].text}')

        except AttributeError as ae:
            logger.exception(f'Problem reading response attributes!'
                                  f'\n{ae.args}')

    def get_next_token(self):
        try:
            assert self.response is not None
        except AssertionError:
            logger.exception(
                'self.response is None; did you try paginating without '
                'making the initial connection?')

        next_token = self.response.get_next_token()
        logger.debug(f'Next token: {next_token}')

        if self.next_token==next_token:
            logger.exception(f'Same pagination token returned')

        if next_token is None:
            self.has_next = False

        self.next_token = next_token

        return next_token

    def _auth(self, key, env_key_name):
        """
        TODO: any verification would go here, none is performed as yet
        Verify passed API key
        :param key: (optional) API key in string format
        :param env_key_name: (optional) API key inside a .env variable inside
          the */twitter_connection/ directory
        :return: verified key
        """
        try:
            if key is None and env_key_name is None:
                raise AssertionError('Neither a key nor a key name for local '\
                                     'environment variable were passed')

            token = key if key is not None else config(env_key_name)

            logger.debug(f'Authorizing connection: '
                         f'"{"direct key" if key is not None else env_key_name}"')

            return token

        except UndefinedValueError as env_e:
            logger.exception(f'The passed key name was not found: {env_e.args}')
        except AssertionError as e:
            logger.exception(e.args)

    # Makes a get request and returns the response
    def _connect_to_endpoint(self, url, headers):
        """Makes a get request and returns the response"""
        response = requests.request('GET', url, headers=headers)

        if response.status_code != 200:
            raise ConnectionError(response)

        return response


if __name__ == '__main__':
    con = TwitterConnection('es', True, key_name='SECRET')
    con.paginate('.', ('test', 'test'), 100, 1)