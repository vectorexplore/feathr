from feathr import FeathrClient, FeatureQuery, ObservationSettings
from loguru import logger

from fraud_detection_2_features import FraudDetectionSchema as sc


# client.build_features be called first 
def get_offline_features(client: FeathrClient, observation_path: str, output_path: str):
    # To extract the offline feature values from the features that have different keys, we use multiple `FeatureQuery` objects.

    account_feature_query = FeatureQuery(
        feature_list=sc.account_feature_names,
        key=sc.account_id,
    )

    transactions_feature_query = FeatureQuery(
        feature_list=sc.transactions_feature_names,
        key=sc.transaction_id,
    )

    derived_feature_query = FeatureQuery(
        feature_list=sc.derived_feature_names,
        key=[sc.transaction_id, sc.account_id],
    )

    # for demo only, use the same transaction file.                   
    settings = ObservationSettings(
        observation_path=observation_path,
        event_timestamp_column="timestamp",
        timestamp_format="yyyyMMdd HHmmss",
    )

    client.get_offline_features(
        observation_settings=settings,
        feature_query=[account_feature_query, transactions_feature_query, derived_feature_query],
        output_path=output_path,
    )
    client.wait_job_to_finish(timeout_sec=5000)

    logger.info('please check offline feathers:{}', output_path)


