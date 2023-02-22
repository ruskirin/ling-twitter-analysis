import requests
from logging import getLogger
from decouple import config, UndefinedValueError
from time import sleep
from response import Response
from utils import get_config, get_project_root, get_relative_to_proot


logger = getLogger(__name__)


class TwitterConnection:
    """
    Single connection to the twitter api

    :param lang: specify language of connection
    :param username: name of API key, formatted as
       "<uppercase_username>_KEY=<key>" in a .env file
    :param is_archive: is this a 'Full-Archive Search'? If not, then is
       'Recent Search'
    """
    def __init__(self,
                 lang: str,
                 is_archive: bool = False,
                 key: str = None,
                 key_name: str = None):

        # TODO 2/22: move configuration logic to utils/config.py
        #   and do all reading/writing from there

        self.conf = get_config('conn')

        self.lang = lang
        self.is_archive = is_archive
        self.header = self.create_headers(key, key_name)

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
                 query: tuple,
                 batch_size=1000,
                 num_batches=1,
                 sleep_sec=0):

        # TODO 2/21: verify files are being saved properly

        # TODO 2/21: at the moment the saved batches vary in size and go over
        #   the requested amount, especially when batch_size is closer to 100
        #   as querying for 100 tweets isn't guaranteed to (and most of the time
        #   doesn't) return exactly 100 tweets. Current accumulates the response
        #   data unt

        """
        Make a series of .connect() calls until the desired @batch_size is reached
          or no more results available

        :param save_path: location to save extracted tweets
        :param query: (name, topic) pair
          save will include name in filename
          topic is used for the query
        :param batch_size: tweets extracted between saves
        :param num_batches: amount of batches to extract
        :param sleep_sec: time (seconds) between consecutive queries
        :return: int saved pages
        """

        logger.info(f'Starting pagination of: {query[0]}')
        logger.debug(f'Requested: {num_batches} batches of size {batch_size}'
                     f'\nPagination save path: {get_relative_to_proot(save_path)}')

        response = self.connect(query)
        tokens = 0
        batches = 1

        while (response.next_token is not None) and (batches <= num_batches):
            # break between queries if necessary
            sleep(sleep_sec)

            # if batch filled, save
            if len(response) >= batch_size:
                logger.debug(f'Saving batch.')
                response.save_csv(save_path, batch=batches)
                tokens += len(response) # update extracted token count
                batches += 1

                # exits out of function; otherwise double saves
                if batches > num_batches:
                    logger.info(
                        f'Pagination finished; retrieved {tokens} tokens')
                    return batches

                new_response = self.connect(query, response.next_token)
                # clear previous responses to save memory
                response = new_response

            else:
                new_response = self.connect(query, response.next_token)
                response.append(new_response)

        response.save_csv(save_path, batch=batches)
        tokens += len(response)  # update extracted token count

        logger.info(f'Pagination finished; retrieved {tokens} tokens')
        return batches

    def connect(self, query_topic, next_token=None) -> Response:
        """
        Make a single connection and return response

        :param query_topic: just the TOPIC of your query -- fields and "rules" have already
          been set through .set_fields()
        :param next_token: token for next page
        :return: response.Response object
        """
        url = self.create_url(query_topic[1], next_token)
        logger.debug(f'URL: {url}')

        try:
            r = self._connect_to_endpoint(url, self.header)
            response = Response(
                lang=self.lang,
                topic=query_topic[0],
                response=r.json()
            )
            return response

        except ConnectionError as ce:
            if ce.args[0].status_code == 429:
                logger.exception('Too many requests! Pausing for 5 seconds...')
                sleep(5)

                return self.connect(query_topic, next_token)
            else:
                logger.exception(f'{ce.args[0].status_code}\n'
                                      f'{ce.args[0].text}')
                raise

        except AttributeError as ae:
            logger.exception(f'Problem reading response attributes!'
                                  f'\n{ae.args}')
            raise

    def _auth(self, key, env_key_name):
        """
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

    def _connect_to_endpoint(self, url, headers):
        """Makes a get request and returns the response"""
        response = requests.request('GET', url, headers=headers)

        if response.status_code != 200:
            raise ConnectionError(response)

        return response


if __name__ == '__main__':
    con = TwitterConnection('es', True, key_name='SECRET')
    con.paginate(
        get_project_root()/'src',
        query=('test', 'test'),
        batch_size=500,
        num_batches=2,
        sleep_sec=1)