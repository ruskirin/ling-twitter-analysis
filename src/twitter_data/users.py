from logging import getLogger
from twitter_data.twitter_data import TwitterData

logger = getLogger(__name__)


class Users(TwitterData):
    def __init__(self, data, topic, lang):
        super().__init__(data, topic, lang)

        # if data is not None:
        #     self.rename_cols(conf['rename_maps']['users'])