#!/usr/bin/env python
# coding: utf-8

# # Feathr Fraud Detection Sample
# 
# This notebook illustrates the use of Feature Store to create a model that predicts the fraud status of transactions based on the user account data and trasaction data. The main focus of this notebook is to depict:
# * How a feature designer can define heterogenious features from different data sources (user account data and transaction data) with different keys by using Feathr, and
# * How a feature consumer can extract features using multiple `FeatureQuery`.
# 
# The sample fraud transaction datasets that are used in the notebook can be found here: https://github.com/microsoft/r-server-fraud-detection.
# 
# The outline of the notebook is as follows: 
# 1. Setup Feathr environment
# 2. Initialize Feathr client 
# 3. Define features
# 4. Build features and extract offline features
# 5. Build a fraud detection model
# 6. Materialize features

# ## 1. Setup Feathr Environment

# ## 2. Initialize Feathr Client

import os
import sys
from pathlib import Path
from loguru import logger

import numpy as np
import pandas as pd

from feathr import (
    FeatureAnchor,
    HdfsSource,
)
from feathr.utils.job_utils import get_result_df
from feathr.utils._env_config_reader import EnvConfigReader

from feathr import FeathrClient

DO_GET_OFFLINE = False
DO_MODEL = False
DO_MATERIALIZE = True


PROJECT_NAME = "fraud_detection"

# To use python parameter
feathr_config_path = './feathr_config.yaml'

local_workspace_dir = '/tmp/feathr-demo-local-workspace'
client = FeathrClient(feathr_config_path,local_workspace_dir)

from fraud_detection_1_data import prepare_data
(account_info_source_path, transactions_source_path) = prepare_data(PROJECT_NAME, client)

# Here, we use `accountCountry`, `isUserRegistered`, `numPaymentRejects1dPerUser`, and `accountAge` as the account features.
def account_preprocessing(df):
    """Drop rows with missing values in the account info dataset."""
    return df.select(
        "accountID",
        "accountCountry",
        "isUserRegistered",
        "numPaymentRejects1dPerUser",
        "accountAge",
    ).dropna(subset=["accountID"])

account_info_source = HdfsSource(
    name="account_data",
    path=account_info_source_path,
    preprocessing=account_preprocessing,
)

from fraud_detection_2_features import FraudDetectionSchema as sc

account_anchor = FeatureAnchor(
    name="account_features",
    source=account_info_source,
    features=sc.account_features,
)

def transaction_preprocessing(df):
    """Preprocess the transaction data."""
    import pyspark.sql.functions as F

    return df.withColumn("ipCountryCode", F.upper("ipCountryCode"))


transactions_source = HdfsSource(
    name="transaction_data",
    path=transactions_source_path,
    event_timestamp_column="timestamp",
    timestamp_format="yyyyMMdd HHmmss",
    preprocessing=transaction_preprocessing,
)

transaction_feature_anchor = FeatureAnchor(
    name="transaction_features",
    source=transactions_source,
    features=sc.transaction_features,
)

agg_anchor = FeatureAnchor(
    name="transaction_agg_features",
    source=transactions_source,
    features=sc.agg_features,
)

# ## 4. Build Features and Extract Offline Features
client.build_features(
    anchor_list=[
        account_anchor,
        transaction_feature_anchor,
        agg_anchor,
    ],
    derived_feature_list=sc.derived_features,
)

# ----------------------------------------------
# register features
# ----------------------------------------------
try:
    client.register_features()
except Exception as e:
    print(e)
logger.info(client.list_registered_features(project_name=client.project_name))

#################################
#  Step 3
#################################
if DO_GET_OFFLINE:
    # prepare get offline output path 
    env_config = EnvConfigReader(config_path=feathr_config_path)
    dfs_workspace_dir = env_config.get('spark_config__local__dfs_workspace')
    offline_fea_output_path = transactions_source_path.rpartition("/")[0] + f"/fraud_transactions_features"
    offline_full_path = offline_fea_output_path

    # get offline features for training
    from fraud_detection_3_get_offline import get_offline_features

    # Use the same path as observation for demo only
    # In production setting:
    #   transaction source(e.g. all bank credit transactions) is normally very big
    #   and observation(e.g. transaction with feedback) is a small fraction of it. 
    observation_path = transactions_source_path

    get_offline_features(client, observation_path, offline_full_path)

#################################
#  Step 4
#################################
if DO_GET_OFFLINE and DO_MODEL: # depend on get offline
    # pull offline features to local machine
    df = get_result_df(client)[
        sc.all_feature_names
        + ["is_fraud", "timestamp"]
    ]

    print(df.head(5))

    # training model
    from fraud_detection_4_model import train
    train(df)

#################################
#  Step 5
#################################
# According to the model result, these features seems good.
# Now, we materialize features to `RedisSink` so that we can retrieve online features.
if DO_MATERIALIZE:
    from fraud_detection_5_materialize import materialize_features
    materialize_features(client)

#################################
#  Step 6
#################################
# run the following to check if online store(e.g. redis) feature is ready
# bash run_sample.sh fraud_detection_6_get_online.py

# Cleaning up the output files. CAUTION: this maybe dangerous if you "reused" the project name.
# import shutil
# shutil.rmtree(WORKING_DIR, ignore_errors=False)


