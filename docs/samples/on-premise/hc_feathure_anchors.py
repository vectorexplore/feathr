from feathr import FeatureAnchor, HdfsSource

#data_home='/public-data/home-credit-default-risk'
data_home='/public-data/home-credit-default-risk-small'
application_train_path=data_home+'/application_train.csv'

def application_preprocessing(df):
    """Preprocess the transaction data."""
    import pyspark.sql.functions as F

    return df.withColumn("CODE_GENDER", F.lower("CODE_GENDER"))

application_source = HdfsSource(
    name="hc_application",
    path=application_train_path,
    # preprocessing=application_preprocessing,
    event_timestamp_column="timestamp",
    timestamp_format="yyyy-MM-dd",
)

from hc_features import HcDefaultRiskSchema as sc
application_anchor = FeatureAnchor(
    name="application_features",
    source=application_source,
    features=sc.application_features,
)

bureau_path=data_home+'/bureau.csv'
bureau_source = HdfsSource(
    name="hc_bureau", 
    path=bureau_path,    
    event_timestamp_column="timestamp",
    timestamp_format="yyyy-MM-dd",
)
agg_bureau_anchor = FeatureAnchor(
    name="agg_bureau_features",
    source=bureau_source,
    features=sc.agg_bureau_features,    
)


# bureau_balance
# bb_path=data_home+'/bureau_balance.csv'
# bb_source = HdfsSource(
#     name="hc_bureau_balance", 
#     path=bb_path,    
#     event_timestamp_column="timestamp",
#     timestamp_format="yyyy-MM-dd",
# )
# agg_bb_anchor = FeatureAnchor(
#     name="agg_bb_features",
#     source=bb_source,
#     features=sc.agg_bb_features,    
# )


# def transaction_preprocessing(df):
#     """Preprocess the transaction data."""
#     import pyspark.sql.functions as F

#     return df.withColumn("ipCountryCode", F.upper("ipCountryCode"))
