from src.twitter_data import twitter_data as td
import files
import pytest


@pytest.fixture(scope='module')
def tweet_object():
    path = files.get_save_path('e') \
           /'2023-02-22-at-18:46:25' \
           / 'tweets' \
           / 'sample-es-twitter-decir-tweets-1-589.csv'
    return td.TwitterData.from_csv(path, 'es')


# def test_duplicate_detect(tweet_object):
#     true_dups = {'1628541852012904455', '1628541760275087361'}
#     ids = tweet_object._read_ids()
#
#     dups = tweet_object._remove_duplicates(ids)
#     assert dups==true_dups

