from src.twitter_data import tweets
from utils import files
import pytest


"""--------------------fixtures--------------------"""
@pytest.fixture(scope='module')
def sample_paths():
    test_path = files.get_project_root()/'tests'
    sample_path = test_path/'samples'

    return test_path, sample_path


@pytest.fixture(scope='module')
def id_keeping_paths(sample_paths):
    _, sample_path = sample_paths

    id_keep_path = sample_path / 'id-keeping'

    data_path = id_keep_path / 'es-twitter-df-sample-tweets-13.csv'
    ids_existing_path = id_keep_path / 'ids-existing.txt'
    ids_write_path = id_keep_path / 'ids-new.txt'
    ids_actual_path = id_keep_path / 'ids-actual.txt'

    yield data_path, ids_existing_path, ids_write_path, ids_actual_path

    # ids_write_path.unlink()


@pytest.fixture(scope='module')
def id_keeping_objects(id_keeping_paths):
    data_path, ids_existing_path, ids_write_path, ids_actual_path = id_keeping_paths

    data = tweets.Tweets.from_csv(data_path, 'es', topic='actual')
    data_ids = set(str(i) for i in data.d['id'].unique())
    actual_ids = set(ids_actual_path.read_text().split())

    yield data, data_ids, actual_ids


@pytest.fixture(scope='module')
def tweet_object(sample_paths):
    t_path, sample_path, e_path, c_path = sample_paths

    data_name = 'es-parecer-original-tweets-10083-0.csv'
    data_dir = '2021-11-07-at-22:10:00'
    data_path = e_path/data_dir/data_name

    d = tweets.Tweets.from_csv(data_path, 'es')

    yield d

    # teardown

    # TODO 4/4/2023: not very kosher as this assumes where the data will be
    #   saved to. Instead, perhaps return the name_scheme as well and use that
    #   in save()?
    dpath = d.save(sample_path, 'csv', name_scheme='sample-data')
    dpath[0].unlink()


"""--------------------tests--------------------"""
def test_update_ids(id_keeping_paths, id_keeping_objects):
    _, ids_existing_path, ids_write_path, _ = id_keeping_paths
    data, data_ids, actual_ids = id_keeping_objects

    data.update_ids(ids_existing_path, ids_write_path)
    written_ids = set(ids_write_path.read_text().split())

    assert written_ids == actual_ids


def test_remove_ids():
    pass


def test_write_read_ids(id_keeping_paths, id_keeping_objects):
    _, _, ids_write_path, _ = id_keeping_paths
    data, data_ids, _ = id_keeping_objects

    data._write_ids(ids_write_path)
    read_ids = data._read_ids(ids_write_path)

    assert data_ids == read_ids


def test_save(sample_paths, tweet_object):
    t_path, sample_path, e_path, c_path = sample_paths
    d = tweet_object

    dpath = d.save(sample_path, 'csv', name_scheme='sample-data')

    d2 = tweets.Tweets.from_csv(dpath[0],  'es', topic='sample-data-2')

    assert d.d.equals(d2.d)


# def test_duplicate_detect(tweet_object):
#     true_dups = {'1628541852012904455', '1628541760275087361'}
#     ids = tweet_object._read_ids()
#
#     dups = tweet_object._remove_duplicates(ids)
#     assert dups==true_dups

