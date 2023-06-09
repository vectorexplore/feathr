# Goal of this file is to run a basic Feathr script within spark so that Maven packages can be downloaded into the docker container to save time during actual run.
# This can also serve as a sanity check

import os
from datetime import datetime
import pandas as pd
from feathr import FeathrClient
from feathr import BOOLEAN, FLOAT, INT32, ValueType
from feathr import Feature, DerivedFeature, FeatureAnchor
from feathr import HdfsSource
from feathr import TypedKey
from pyspark.sql import DataFrame
from pathlib import Path
from feathr import RedisSink, MaterializationSettings
from loguru import logger
from feathr.utils._env_config_reader import EnvConfigReader
from feathr.datasets.utils import maybe_download
from feathr.datasets.constants import NYC_TAXI_SMALL_URL
from pyarrow import fs

# ----------------------------------------------
# 1. prepare config
# ----------------------------------------------

# config
feathr_config_path = "./feathr_config.yaml"

# local workspace and dfs workspace
local_workspace_dir = '/tmp/feathr-demo-workspace'
env_config = EnvConfigReader(config_path=feathr_config_path)
dfs_prefix = env_config.get('spark_config__local__dfs_prefix')
dfs_workspace_dir = env_config.get('spark_config__local__dfs_workspace')

client = FeathrClient(feathr_config_path, local_workspace_dir)

# ----------------------------------------------
# 2. prepare data
# ----------------------------------------------
DATA_FILE_PATH = "/data/green_tripdata_2020-04_with_index.csv"
TIMESTAMP_COL = "lpep_dropoff_datetime"
TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"

# NYC_TAXI_SMALL_URL is used for both agg feathers and observation setting.
local_data_file = local_workspace_dir + DATA_FILE_PATH
maybe_download(src_url=NYC_TAXI_SMALL_URL, dst_filepath=local_data_file)

# copy to hdfs
hdfs_data_file = dfs_workspace_dir + DATA_FILE_PATH
fs.copy_files(local_data_file,  dfs_prefix + hdfs_data_file)


# ----------------------------------------------
# 3. preprocessing
# ----------------------------------------------

def preprocessing(df: DataFrame) -> DataFrame:
    import pyspark.sql.functions as F
    df = df.withColumn("fare_amount_cents",
                       (F.col("fare_amount") * 100.0).cast("float"))
    return df

# ----------------------------------------------
# 4. define data source
# ----------------------------------------------
batch_source = HdfsSource(
    name="nycTaxiBatchSource",
    path=hdfs_data_file,
    event_timestamp_column=TIMESTAMP_COL,
    preprocessing=preprocessing,
    timestamp_format=TIMESTAMP_FORMAT,
)

# ----------------------------------------------
# 5.1.1 define Feature, FeatureAnchor
# ----------------------------------------------

trip_key = TypedKey(
    key_column="trip_id",
    key_column_type=ValueType.INT32,
    description="trip id",
    full_name="nyc_taxi.trip_id",
)

f_trip_distance = Feature(
    name="f_trip_distance",
    key=trip_key,
    feature_type=FLOAT,
    transform="trip_distance",
)
f_trip_time_duration = Feature(
    name="f_trip_time_duration",
    key=trip_key,
    feature_type=FLOAT,
    transform="cast_float((to_unix_timestamp(lpep_dropoff_datetime) - to_unix_timestamp(lpep_pickup_datetime)) / 60)",
)


features = [
    f_trip_distance,
    f_trip_time_duration,
    Feature(
        name="f_is_long_trip_distance",
        key=trip_key,
        feature_type=BOOLEAN,
        transform="trip_distance > 30.0",
    ),
    Feature(
        name="f_day_of_week",
        key=trip_key,
        feature_type=INT32,
        transform="dayofweek(lpep_dropoff_datetime)",
    ),
    Feature(
        name="f_day_of_month",
        key=trip_key,
        feature_type=INT32,
        transform="dayofmonth(lpep_dropoff_datetime)",
    ),
    Feature(
        name="f_hour_of_day",
        key=trip_key,
        feature_type=INT32,
        transform="hour(lpep_dropoff_datetime)",
    ),
    Feature(
        name="f_minute_of_day",
        key=trip_key,
        feature_type=INT32,
        transform="minute(lpep_dropoff_datetime)",
    ),
]

# After you have defined features, bring them together to build the anchor to the source.
feature_anchor = FeatureAnchor(
    name="feature_anchor",
    #source=INPUT_CONTEXT,  # for observation data.
    source=batch_source, 
    features=features,
)

f_trip_time_distance = DerivedFeature(name="f_trip_time_distance",
                                      feature_type=FLOAT,
                                      key=trip_key,
                                      input_features=[
                                          f_trip_distance, f_trip_time_duration],
                                      transform="f_trip_distance * f_trip_time_duration")

f_trip_time_rounded = DerivedFeature(name="f_trip_time_rounded",
                                     feature_type=INT32,
                                     key=trip_key,
                                     input_features=[f_trip_time_duration],
                                     transform="f_trip_time_duration % 10")

derived_feature = [f_trip_time_distance, f_trip_time_rounded]


client.build_features(
    anchor_list=[feature_anchor],
    derived_feature_list=derived_feature,
)

feature_names = [feature.name for feature in features + derived_feature]
logger.info(feature_names)


# ----------------------------------------------
# 7. register features
# ----------------------------------------------
try:
    client.register_features()
except Exception as e:
    print(e)
logger.info(client.list_registered_features(project_name=client.project_name))

# ----------------------------------------------
# 8. define materialize sink
# ----------------------------------------------
redisSink = RedisSink(table_name="nycTaxiDemoFeature")
# Materialize two features into a redis table.
settings = MaterializationSettings("nycTaxiMaterializationJob",
                                   sinks=[redisSink],
                                   feature_names=feature_names)

# ----------------------------------------------
# 9. call spark_submit to run spark jobs
# ----------------------------------------------

# option 1: config here
# spark_conf={'spark.driver.host':'192.168.122.1', 'spark.driver.bindAddress':'192.168.122.1', 'deploy-mode':'client','spark.yarn.archive':dfs_prefix+'/share/spark340-hadoop3-jars.zip'}
# client.materialize_features(settings, execution_configurations=spark_conf, allow_materialize_non_agg_feature=True)
# 
# option 2: use config from $SPARK_HOME/conf/spark-default.conf
client.materialize_features(settings, allow_materialize_non_agg_feature=True)

client.wait_job_to_finish(timeout_sec=5000)

# ----------------------------------------------
# 10. check materialized features
# ----------------------------------------------

