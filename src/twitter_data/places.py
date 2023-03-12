from logging import getLogger
from twitter_data import TwitterData

logger = getLogger(__name__)


class Places(TwitterData):
    def __init__(self, data, topic, lang):
        super().__init__(data, topic, lang)

        # self.rename_cols(conf['rename_maps']['places'])