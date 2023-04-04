from src.twitter_data import tweets
import files
import pytest


@pytest.fixture(scope='module')
def tweet_object():
    s1 = files.get_save_path('e')
    s2 = files.get_save_path('e') / '2021-11-07-at-22:10:00'
    p1 = s2 / 'es-parecer-original-tweets-10083-0.csv'

    d1 = tweets.Tweets.from_csv(p1, 'es')
    dp = d1.save(s1, 'csv', name_scheme='parecer-sample-from-2021-11-07-22:10')[0]

    d2 = tweets.Tweets.from_csv(dp, 'es', topic='sample')
    return d1, d2


def test_save(tweet_object):
    d1, d2 = tweet_object

    assert d1.data.equals(d2.data)


# def test_duplicate_detect(tweet_object):
#     true_dups = {'1628541852012904455', '1628541760275087361'}
#     ids = tweet_object._read_ids()
#
#     dups = tweet_object._remove_duplicates(ids)
#     assert dups==true_dups

