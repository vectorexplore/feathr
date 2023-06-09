import os
from datetime import datetime

from loguru import logger

from feathr import FeathrClient, FeatureQuery, ObservationSettings
from feathr.utils.job_utils import get_result_df
from feathr.utils._env_config_reader import EnvConfigReader

from hc_features import HcDefaultRiskSchema as sc


# client.build_features be called first 
def get_offline_features(client: FeathrClient, observation_path: str, output_path: str):
    # To extract the offline feature values from the features that have different keys, we use multiple `FeatureQuery` objects.
    application_feature_query = FeatureQuery(
        feature_list=sc.application_feature_names + sc.agg_bureau_features_names,
        key=sc.loan_id,
    )
    bureau_feature_query = FeatureQuery(
        feature_list=sc.agg_bureau_features_names,
        key=sc.loan_id,
    )
    # for demo only, use the same transaction file.                   
    settings = ObservationSettings(
        observation_path=observation_path, # the path in hdfs
        is_file_path=True,
        event_timestamp_column="timestamp",
        timestamp_format="yyyy-MM-dd",
    )

    # To build features
    # Option 1 (Preferred): call registry api to build
    client.get_features_from_registry(client.project_name)
    # Option 2: use anchor definition
    # from hc_feathure_anchors import application_anchor, agg_bureau_anchor
    # client.build_features(
    #     anchor_list=[
    #         application_anchor,
    #         agg_bureau_anchor,
    #     ],
    # )

    execution_configurations={'spark.feathr.outputFormat': 'parquet'}
    client.get_offline_features(
        observation_settings=settings,
        feature_query=[application_feature_query, bureau_feature_query],
        output_path=output_path,
        execution_configurations=execution_configurations,
    )
    client.wait_job_to_finish(timeout_sec=5000)
    logger.info('please check offline feathers:{}', output_path)

def main():
    PROJECT_NAME = "hc-default-risk.v1"
    os.environ['project_config__project_name'] = PROJECT_NAME

    # To use python parameter
    feathr_config_path = './feathr_config.yaml'
    client = FeathrClient(feathr_config_path)

    # prepare get offline output path 
    dfs_workspace_dir = EnvConfigReader(config_path=feathr_config_path).get('spark_config__local__dfs_workspace')
    offline_full_path = dfs_workspace_dir + "/output-fea-" + datetime.now().strftime('%Y%m%d%H%M%S') 

    # Use the same path as observation for demo only
    observation_path='/public-data/home-credit-default-risk-small/application_train.csv'

    get_offline_features(client, observation_path, offline_full_path)

    # debug
    df = get_result_df(client)[
        sc.all_feature_names + ['SK_ID_CURR']
    ]
    print(df.head(5))

if __name__=="__main__":
    main()
