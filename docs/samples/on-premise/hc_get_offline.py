import os
from loguru import logger

from feathr import FeathrClient, FeatureQuery, ObservationSettings
from feathr.utils.job_utils import get_result_df
from feathr.utils._env_config_reader import EnvConfigReader

from hc_features import HcDefaultRiskSchema as sc
from hc_feathure_anchors import application_anchor, agg_bureau_anchor


# client.build_features be called first 
def get_offline_features(client: FeathrClient, observation_path: str, output_path: str):
    # To extract the offline feature values from the features that have different keys, we use multiple `FeatureQuery` objects.
    application_feature_query = FeatureQuery(
        feature_list=sc.application_feature_names + sc.agg_bureau_features_names,
        key=sc.loan_id,
    )
    # bb_feature_query = FeatureQuery(
    #     feature_list=sc.agg_bb_features_names,
    #     key=sc.bureau_id,
    # )
    # for demo only, use the same transaction file.                   
    settings = ObservationSettings(
        observation_path=observation_path,
        is_file_path=True,
        event_timestamp_column="timestamp",
        timestamp_format="yyyy-MM-dd",
    )

    execution_configurations={'spark.feathr.outputFormat': 'parquet'}



    # ## 4. Build Features and Extract Offline Features
    client.build_features(
        anchor_list=[
            application_anchor,
            agg_bureau_anchor,
            # agg_bb_anchor,
        ],
        # derived_feature_list=sc.derived_features,
    )

    client.get_offline_features(
        observation_settings=settings,
        feature_query=[application_feature_query],#, bb_feature_query],
        output_path=output_path,
        execution_configurations=execution_configurations,
    )
    client.wait_job_to_finish(timeout_sec=5000)

    logger.info('please check offline feathers:{}', output_path)



def main():
    PROJECT_NAME = "hc-default-risk"
    os.environ['project_config__project_name'] = PROJECT_NAME

    # To use python parameter
    feathr_config_path = './feathr_config.yaml'

    # TODO: don't need local workspace dir.
    local_workspace_dir = '/tmp/feathr-demo-local-workspace'
    client = FeathrClient(feathr_config_path,local_workspace_dir)


    # feathr_config_path = './feathr_config.yaml'
    # client = FeathrClient(feathr_config_path)

    # prepare get offline output path 
    env_config = EnvConfigReader(config_path=feathr_config_path)
    dfs_workspace_dir = env_config.get('spark_config__local__dfs_workspace')
    offline_fea_output_path = dfs_workspace_dir + f"/hc_application_features"
    offline_full_path = offline_fea_output_path

    # Use the same path as observation for demo only
    data_home='/public-data/home-credit-default-risk-small'
    application_train_path=data_home+'/application_train.csv'
    observation_path = application_train_path

    get_offline_features(client, observation_path, offline_full_path)

    # debug
    df = get_result_df(client)[
        sc.all_feature_names + ['SK_ID_CURR']
    ]
    print(df.head(5))

if __name__=="__main__":
    main()
