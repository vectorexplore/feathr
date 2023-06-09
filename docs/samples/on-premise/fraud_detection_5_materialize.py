from datetime import datetime, timedelta
from feathr import FeathrClient, BackfillTime, MaterializationSettings, RedisSink
from loguru import logger
from fraud_detection_2_features import FraudDetectionSchema as sc

# Please set Redis password first
# os.environ['REDIS_PASSWORD']='xxxxx'

# client.build_features be called first 
def materialize_features(client: FeathrClient):

    backfill_time = BackfillTime(
        start=datetime(2013, 8, 4),
        end=datetime(2013, 8, 4),
        step=timedelta(days=1),
    )

    ACCOUNT_FEATURE_TABLE_NAME = "fraudDetectionAccountFeatures" 
    client.materialize_features(
        MaterializationSettings(
            ACCOUNT_FEATURE_TABLE_NAME,
            backfill_time=backfill_time,
            sinks=[RedisSink(table_name=ACCOUNT_FEATURE_TABLE_NAME)],
            feature_names=sc.account_feature_names,
        ),
        allow_materialize_non_agg_feature=True,
    )
    client.wait_job_to_finish(timeout_sec=5000)

    TRAN_FEATURE_TABLE_NAME = "fraudDetectionTranFeatures" 
    client.materialize_features(
        MaterializationSettings(
            TRAN_FEATURE_TABLE_NAME,
            backfill_time=backfill_time,
            sinks=[RedisSink(table_name=TRAN_FEATURE_TABLE_NAME)],
            feature_names=sc.transactions_feature_names,
        ),
        allow_materialize_non_agg_feature=True,
    )
    client.wait_job_to_finish(timeout_sec=5000)