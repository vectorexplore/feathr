import os
from datetime import datetime
import pandas as pd
from feathr import FeathrClient
from feathr import BOOLEAN, FLOAT, INT32, ValueType
from feathr import Feature, DerivedFeature, FeatureAnchor
from feathr import FeatureQuery, ObservationSettings
from feathr import INPUT_CONTEXT, HdfsSource
from feathr import WindowAggTransformation
from feathr import TypedKey
from pyspark.sql import DataFrame
from pathlib import Path
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
# Here use input context

# ----------------------------------------------
# 5.1.1 define Feature, FeatureAnchor
# ----------------------------------------------
f_trip_distance = Feature(
    name="f_trip_distance",
    feature_type=FLOAT,
    transform="trip_distance",
)
f_trip_time_duration = Feature(
    name="f_trip_time_duration",
    feature_type=FLOAT,
    transform="cast_float((to_unix_timestamp(lpep_dropoff_datetime) - to_unix_timestamp(lpep_pickup_datetime)) / 60)",
)

features = [
    f_trip_distance,
    f_trip_time_duration,
    Feature(
        name="f_is_long_trip_distance",
        feature_type=BOOLEAN,
        transform="trip_distance > 30.0",
    ),
    Feature(
        name="f_day_of_week",
        feature_type=INT32,
        transform="dayofweek(lpep_dropoff_datetime)",
    ),
    Feature(
        name="f_day_of_month",
        feature_type=INT32,
        transform="dayofmonth(lpep_dropoff_datetime)",
    ),
    Feature(
        name="f_hour_of_day",
        feature_type=INT32,
        transform="hour(lpep_dropoff_datetime)",
    ),
]

# ----------------------------------------------
# 5.1.2 After you have defined features, bring them together to build the anchor to the source.
# ----------------------------------------------
feature_anchor = FeatureAnchor(
    name="feature_anchor",
    source=INPUT_CONTEXT,  # Pass through source, i.e. observation data.
    features=features,
)

# ----------------------------------------------
# 5.2 Derived Features
# ----------------------------------------------
f_trip_time_distance = DerivedFeature(name="f_trip_time_distance",
                                      feature_type=FLOAT,
                                      input_features=[
                                          f_trip_distance, f_trip_time_duration],
                                      transform="f_trip_distance * f_trip_time_duration")

f_trip_time_rounded = DerivedFeature(name="f_trip_time_rounded",
                                     feature_type=INT32,
                                     input_features=[f_trip_time_duration],
                                     transform="f_trip_time_duration % 10")

derived_feature = [f_trip_time_distance, f_trip_time_rounded]



# ----------------------------------------------
# 5.3.1 define data source for agg feathers
# ----------------------------------------------
batch_source = HdfsSource(
    name="nycTaxiBatchSource",
    path=hdfs_data_file, # no hdfs: prefix
    event_timestamp_column=TIMESTAMP_COL,
    preprocessing=preprocessing,
    timestamp_format=TIMESTAMP_FORMAT,
)

# ----------------------------------------------
# 5.3.2 key for agg features
# ----------------------------------------------
agg_key = TypedKey(
    key_column="DOLocationID",
    key_column_type=ValueType.INT32,
    description="location id in NYC",
    full_name="nyc_taxi.location_id",
)

agg_window = "90d"

# ----------------------------------------------
# 5.3.3 features with aggregations
# ----------------------------------------------
agg_features = [
    Feature(
        name="f_location_avg_fare",
        key=agg_key,
        feature_type=FLOAT,
        transform=WindowAggTransformation(
            agg_expr="fare_amount_cents",
            agg_func="AVG",
            window=agg_window,
        ),
    ),
    Feature(
        name="f_location_max_fare",
        key=agg_key,
        feature_type=FLOAT,
        transform=WindowAggTransformation(
            agg_expr="fare_amount_cents",
            agg_func="MAX",
            window=agg_window,
        ),
    ),
]

# ----------------------------------------------
# 5.3.4 FeatureAnchor for agg features
# ----------------------------------------------
agg_feature_anchor = FeatureAnchor(
    name="agg_feature_anchor",
    # External data source for feature. Typically a data table.
    # here use the same data file
    source=batch_source,
    features=agg_features,
)


# ----------------------------------------------
# 6. build feature meta data
# ----------------------------------------------
client.build_features(
    anchor_list=[feature_anchor, agg_feature_anchor],
    derived_feature_list=derived_feature,
)

feature_names = [feature.name for feature in features + agg_features]
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
# 8.1 get offline features
# ----------------------------------------------
now = datetime.now().strftime("%Y%m%d%H%M%S")
offline_features_path = os.path.join("debug", f"test_output_{now}")

# Features that we want to request. Can use a subset of features
# TODO: CHECK
query = FeatureQuery(
    feature_list=feature_names,
    key=agg_key,
)
settings = ObservationSettings(
    observation_path=hdfs_data_file, # no hdfs prefix
    event_timestamp_column=TIMESTAMP_COL,
    timestamp_format=TIMESTAMP_FORMAT,
)


# ----------------------------------------------
# 9. call spark_submit to run spark jobs
# ----------------------------------------------

# sample way to config spark config
# execution_configurations = {}
# spark_yarn_archive  = env_config.get('spark_config__local__spark_yarn_archive')
# if spark_yarn_archive is not None:
#     execution_configurations['spark.yarn.archive'] = spark_yarn_archive
# logger.info(execution_configurations)

client.get_offline_features(
    observation_settings=settings,
    feature_query=query,
    output_path = dfs_workspace_dir + '/' + offline_features_path, # no hdfs: prefix
    # execution_configurations=execution_configurations,
)
client.wait_job_to_finish(timeout_sec=5000)

# ----------------------------------------------
# 10. get offline feathers for further model building
# ----------------------------------------------

# copy to local for further debug
# local_dir = os.path.join(local_workspace_dir, offline_features_path)
# os.makedirs(local_dir, exist_ok=True)
# fs.copy_files(dfs_workspace_dir + '/' + offline_features_path, local_dir)

# or view directly
from feathr.utils.job_utils import get_result_df
res_df = get_result_df(client)
print(res_df)

