import re
import requests


class TwitterConnection:
    # Option of supplying either a filepath to bearer token or the direct
    #   string form of it; otherwise will default to predefined path
    def __init__(self, lang, is_archive=False, cred_path='', bearer_token=''):
        self._langs = {"russian": "ru", "english": "en", "spanish": "es"}

        self._prefix_search_recent = 'https://api.twitter.com/2/tweets/search/recent?query'
        self._prefix_search_archive = 'https://api.twitter.com/2/tweets/search/all?query'

        self.lang = self._langs[lang] \
            if lang in self._langs.keys() \
            else self._langs["english"]
        # Do a full-archive search?
        self.prefix = self._prefix_search_recent if is_archive==False \
                      else self._prefix_search_archive

        # TXT filepath with Bearer token saved as:
        #   'BEARER "XXXYYYZZZ..."'
        # NOTE: must be prepended with 'BEARER'
        self.cred_path = cred_path if len(cred_path)>0 \
            else r'../twitter-connection/credentials.txt'

        # Alternative option to giving a filepath
        self.bearer_token = bearer_token

        self.header = self.create_headers()

        self.query_cond = ''
        self.fields_tweet = ''
        self.fields_expan = ''
        self.fields_user = ''
        self.fields_place = ''

        self.url = ''

        self.response = None
        self.next_token = ''

    # If raw token was passed during object creation, use that
    #   else look for a token in @cred_path
    def auth(self):
        if len(self.bearer_token)>0:
            return self.bearer_token

        try:
            with open(self.cred_path, 'r') as c:
                f = c.read()

                token = re.search(r'BEARER[\w\s]+[\'\"]?(.+)[\'\"]?\b', f).group(1)
                return token
        except IOError:
            print(f'Invalid file path: {self.cred_path}')
        except Exception:
            print(f'Invalid bearer token passed!')

    # Get proper format for bearer token
    def create_headers(self):
        return {'Authorization': f'Bearer {self.auth()}'}

    # Combine query, fields and (if available) next_token into a proper URL
    def create_url(self, topic, next_token=None):
        if next_token is not None:
            self.url = f'{self.prefix}={topic}' \
                       f'&{next_token}' \
                       f'&{self.fields_tweet}' \
                       f'&{self.fields_expan}' \
                       f'&{self.fields_user}' \
                       f'&{self.fields_place}'
        else:
            self.url = f'{self.prefix}={topic}' \
                       f'&{self.fields_tweet}' \
                       f'&{self.fields_expan}' \
                       f'&{self.fields_user}' \
                       f'&{self.fields_place}'

    def connect(self, query, is_next: bool = False):
        response = None

        # if is_next:
        #     if len(self.next_token) == 0:
        #         self.create_url(query, fields)
        #     else:
        #         self.create_url(query, fields,
        #                     f'next_token={self.next_token}')
        # else:
        #     self.create_url(query, fields)

        if is_next and (len(self.next_token) > 0):
            self.create_url(query, f'next_token={self.next_token}')
        else:
            self.create_url(query)

        try:
            response = self._connect_to_endpoint(self.url, self.header)
        except ConnectionError as e:
            print(f"Error establishing connection:\n\n "
                  f"{e.args[0].status_code}\n"
                  f"{e.args[0].text}")

        self.response = response.json()
        return self.get_next_token()

    # !!! PRIVATE !!!
    # Makes a get request and returns the response
    def _connect_to_endpoint(self, url, headers):
        response = requests.request('GET', url, headers=headers)

        if response.status_code != 200:
            raise ConnectionError(response)

        return response

    def get_next_token(self):
        try:
            if self.response['meta']['next_token'] == self.next_token:
                raise Exception("Repeated token")

            self.next_token = self.response['meta']['next_token']
            return True

        except Exception as e:
            print(f"No next token! {e.args[0]}")
            self.next_token = '' # Reset variable

            return False

    def set_query(self, conditions=None):
        if conditions is not None:
            self.query_cond = conditions

    def set_fields(self, tweet=None, expansions=None, user=None, place=None):
        if tweet is not None:
            self.fields_tweet = tweet
        if expansions is not None:
            self.fields_expan = expansions
        if user is not None:
            self.fields_user = user
        if place is not None:
            self.fields_place = place