import re
import requests


class TwitterConnection:
    def __init__(self, lang, cred_path):
        self._langs = {"russian": "ru", "english": "en", "spanish": "es"}

        self.lang = self._langs[lang] \
            if lang in self._langs.keys() \
            else self._langs["english"]

        # Text file with Bearer token saved as:
        #   'BEARER ...'
        self.cred_path = cred_path

        self.header = self.create_headers()
        self.url = ''

        self.response = None
        self.next_token = ''

    # Retrieve bearer token from @self.cred_path
    def auth(self):
        try:
            with open(self.cred_path, 'r') as c:
                f = c.read()

                # TODO ??? : Add case where just token is passed instead to avoid
                #   necessity for specific format of cred files
                token = re.search(r'BEARER[\w\s]+\'?(.+)\'?\b', f).group(1)
                return token
        except IOError:
            print(f'Invalid file path: {self.cred_path}')

    # Get proper format for bearer token
    def create_headers(self):
        return {'Authorization': f'Bearer {self.auth()}'}

    # Combine query, fields and -if available- next_token into a proper URL
    def create_url(self, query, fields, next_token=None):
        prefix_search_recent = 'https://api.twitter.com/2/tweets/search/recent?query'

        if next_token is not None:
            self.url = f'{prefix_search_recent}={query}&{next_token}&{fields}'
        else:
            self.url = f'{prefix_search_recent}={query}&{fields}'

    # !!! PRIVATE !!!
    # Makes a get request and returns the response
    def _connect_to_endpoint(self, url, headers):
        response = requests.request('GET', url, headers=headers)

        if response.status_code != 200:
            raise ConnectionError(response)

        return response

    def connect(self, query, fields, is_next: bool = False):
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
            self.create_url(query, fields,
                            f'next_token={self.next_token}')
        else:
            self.create_url(query, fields)

        try:
            response = self._connect_to_endpoint(self.url, self.header)
        except ConnectionError as e:
            print(f"Error establishing connection:\n\n "
                  f"{e.args[0].status_code}\n"
                  f"{e.args[0].text}")

        self.response = response.json()
        return self.get_next_token()

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