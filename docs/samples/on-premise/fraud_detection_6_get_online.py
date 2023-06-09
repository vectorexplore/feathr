from feathr import FeathrClient
from loguru import logger
from fraud_detection_2_features import FraudDetectionSchema as sc

client = FeathrClient()

ACCOUNT_FEATURE_TABLE_NAME = "fraudDetectionAccountFeatures" 
account_id="A1055520452832600"

materialized_feature_values = client.get_online_features(
    ACCOUNT_FEATURE_TABLE_NAME,
    key=account_id,
    feature_names=sc.account_feature_names,
)
print(type(sc.account_feature_names))
logger.info(dict(zip(['account_id'] + sc.account_feature_names, [account_id] + materialized_feature_values)))
