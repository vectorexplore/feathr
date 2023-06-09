#!/usr/bin/env python
# coding: utf-8

import os
import sys
from pathlib import Path
from loguru import logger

import numpy as np
import pandas as pd

from feathr.utils.job_utils import get_result_df

from feathr import FeathrClient

DO_GET_OFFLINE = False
DO_MODEL = False
DO_MATERIALIZE = False


PROJECT_NAME = "hc-default-risk.v1"
os.environ['project_config__project_name'] = PROJECT_NAME

# To use python parameter
feathr_config_path = './feathr_config.yaml'
client = FeathrClient(feathr_config_path)

# ----------------------------------------------
# register features
# ----------------------------------------------
from hc_feathure_anchors import application_anchor, agg_bureau_anchor
client.build_features(
    anchor_list=[
        application_anchor,
        agg_bureau_anchor,
    ],
)

try:
    client.register_features()
except Exception as e:
    print(e)
logger.info(client.list_registered_features(project_name=client.project_name))


# ----------------------------------------------
#  get offline features
# ----------------------------------------------
if DO_GET_OFFLINE:
    from hc_get_offline import main as get_offline_main
    get_offline_main() 

sys.exit(0)

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


