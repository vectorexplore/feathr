import os
from datetime import datetime, timedelta
from pathlib import Path

from click.testing import CliRunner
from feathr.client import FeathrClient
from feathr.dtype import ValueType
from feathr.job_utils import get_result_df
from feathr.materialization_settings import (BackfillTime,
                                             MaterializationSettings)
from feathr.query_feature_list import FeatureQuery
from feathr.settings import ObservationSettings
from feathr.sink import RedisSink
from feathr.typed_key import TypedKey
from feathrcli.cli import init


# make sure you have run the upload feature script before running these tests
# the feature configs are from feathr_project/data/feathr_user_workspace
def test_feathr_online_store_agg_features():
    """
    Test FeathrClient() get_online_features and batch_get can get data correctly.
    """
    test_workspace_dir = Path(__file__).parent.resolve() / "test_user_workspace"
    os.chdir(test_workspace_dir)
    client = FeathrClient()
    backfill_time = BackfillTime(start=datetime(2020, 5, 20), end=datetime(2020, 5, 20), step=timedelta(days=1))
    redisSink = RedisSink(table_name="nycTaxiDemoFeature")
    settings = MaterializationSettings("nycTaxiTable",
                                   sinks=[redisSink],
                                   feature_names=["f_location_avg_fare", "f_location_max_fare"],
                                   backfill_time=backfill_time)
    client.materialize_features(settings)
    # just assume the job is successful without validating the actual result in Redis. Might need to consolidate
    # this part with the test_feathr_online_store test case
    client.wait_job_to_finish(timeout_sec=600)

    res = client.get_online_features('nycTaxiDemoFeature', '265', ['f_location_avg_fare', 'f_location_max_fare'])
    # just assme there are values. We don't hard code the values for now for testing
    # the correctness of the feature generation should be garunteed by feathr runtime.
    # ID 239 and 265 are available in the `DOLocationID` column in this file:
    # https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2020-04.csv
    # View more detials on this dataset: https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
    assert len(res) == 2
    assert res[0] != None
    assert res[1] != None
    res = client.multi_get_online_features('nycTaxiDemoFeature',
                                    ['239', '265'],
                                    ['f_location_avg_fare', 'f_location_max_fare'])
    assert res['239'][0] != None
    assert res['239'][1] != None
    assert res['265'][0] != None
    assert res['265'][1] != None


def test_feathr_online_store_non_agg_features():
    """
    Test FeathrClient() online_get_features and batch_get can get data correctly.
    """
    test_workspace_dir = Path(__file__).parent.resolve() / "test_user_workspace"
    os.chdir(test_workspace_dir)
    client = FeathrClient()

    client._materialize_features_with_config("feature_gen_conf/test_feature_gen_2.conf")
    # # just assume the job is successful without validating the actual result in Redis. Might need to consolidate
    # # this part with the test_feathr_online_store test case
    client.wait_job_to_finish(timeout_sec=600)
    res = client.get_online_features('nycTaxiDemoFeature', '111', ['f_gen_trip_distance', 'f_gen_is_long_trip_distance',
                                                                   'f1', 'f2', 'f3', 'f4', 'f5', 'f6'])
    # just assme there are values. We don't hard code the values for now for testing
    # the correctness of the feature generation should be garunteed by feathr runtime.
    # ID 239 and 265 are available in the `DOLocationID` column in this file:
    # https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2020-04.csv
    # View more detials on this dataset: https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page

    assert len(res) == 8
    assert res[0] != None
    assert res[1] != None
    # assert constant features
    _validate_constant_feature(res)
    res = client.multi_get_online_features('nycTaxiDemoFeature',
                                           ['239', '265'],
                                           ['f_gen_trip_distance', 'f_gen_is_long_trip_distance', 'f1', 'f2', 'f3', 'f4', 'f5', 'f6'])
    _validate_constant_feature(res['239'])
    assert res['239'][0] != None
    assert res['239'][1] != None
    _validate_constant_feature(res['265'])
    assert res['265'][0] != None
    assert res['265'][1] != None


def _validate_constant_feature(feature):
    assert feature[2] == [10.0, 20.0, 30.0]
    assert feature[3] == ['a', 'b', 'c']
    assert feature[4] == ([1, 2, 3], ['10', '20', '30'])
    assert feature[5] == ([1, 2, 3], [True, False, True])
    assert feature[6] == ([1, 2, 3], [1.0, 2.0, 3.0])
    assert feature[7] == ([1, 2, 3], [1, 2, 3])


def test_feathr_get_offline_features():
    """
    Test get_offline_features() can get data correctly.
    """
    runner = CliRunner()
    with runner.isolated_filesystem():
        runner.invoke(init, [])
        os.chdir('feathr_user_workspace')
        client = FeathrClient()

        location_id = TypedKey(key_column="DOLocationID",
                        key_column_type=ValueType.INT32,
                        description="location id in NYC",
                        full_name="nyc_taxi.location_id")
        feature_query = FeatureQuery(feature_list=["f_location_avg_fare"], key=location_id)
        settings = ObservationSettings(
            observation_path="abfss://feathrazuretest3fs@feathrazuretest3storage.dfs.core.windows.net/demo_data/green_tripdata_2020-04.csv",
            event_timestamp_column="lpep_dropoff_datetime",
            timestamp_format="yyyy-MM-dd HH:mm:ss")
        client.get_offline_features(observation_settings=settings,
            feature_query=feature_query,
            output_path="abfss://feathrazuretest3fs@feathrazuretest3storage.dfs.core.windows.net/demo_data/output.avro")

        vertical_concat_df = get_result_df(client)
        # just assume there are results. Need to think about this test and make sure it captures the result
        assert vertical_concat_df.shape[0] > 1