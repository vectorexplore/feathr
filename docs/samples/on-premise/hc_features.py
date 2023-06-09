# refer: https://www.kaggle.com/code/jsaguiar/lightgbm-with-simple-features

from feathr import (
    STRING, BOOLEAN, FLOAT, INT32, ValueType,
    Feature, DerivedFeature,
    WindowAggTransformation,
    TypedKey,
)

class HcDefaultRiskSchema:
    loan_id = TypedKey(
        key_column="SK_ID_CURR",
        key_column_type=ValueType.INT32,
        description="loan id",
    )

    bureau_id = TypedKey(
        key_column="SK_ID_BUREAU",
        key_column_type=ValueType.INT32,
        description="bureau id",
    )

    application_features = [
        Feature(name="f_days_employed_perc", key=loan_id, feature_type=FLOAT, transform="DAYS_EMPLOYED/DAYS_BIRTH"),
        Feature(name="f_income_credit_perc", key=loan_id, feature_type=FLOAT, transform="AMT_INCOME_TOTAL/AMT_CREDIT"),
        Feature(name="f_income_per_person", key=loan_id, feature_type=FLOAT, transform="AMT_INCOME_TOTAL/CNT_FAM_MEMBERS"),
        Feature(name="f_annuity_income_perc", key=loan_id, feature_type=FLOAT, transform="AMT_ANNUITY/AMT_INCOME_TOTAL"),
        Feature(name="f_payment_rate", key=loan_id, feature_type=FLOAT, transform="AMT_ANNUITY/AMT_CREDIT"),
    ]

    # f_bb_months_balance_min = Feature(name="f_bb_months_balance_min", key=bureau_id, feature_type=FLOAT,
    #     transform=WindowAggTransformation(agg_expr="MONTHS_BALANCE", agg_func="MIN", window="100d"),
    # )

    # agg_bb_features = [
    #     f_bb_months_balance_min,
    #     Feature(name="f_bb_months_balance_max", key=bureau_id, feature_type=FLOAT,
    #         transform=WindowAggTransformation(agg_expr="MONTHS_BALANCE", agg_func="MAX", window="100d"),
    #     ),
    #     Feature(name="f_bb_months_balance_cnt", key=bureau_id, feature_type=FLOAT,
    #         transform=WindowAggTransformation(agg_expr="MONTHS_BALANCE", agg_func="COUNT", window="100d"),
    #     ),
    # ]

    agg_bureau_features = [
        Feature(name="f_days_credit_avg", key=loan_id, feature_type=FLOAT,
            transform=WindowAggTransformation(agg_expr="cast_float(DAYS_CREDIT)", agg_func="AVG", window="100d"),
        ),
        Feature(name="f_days_credit_max", key=loan_id, feature_type=FLOAT,
            transform=WindowAggTransformation(agg_expr="cast_float(DAYS_CREDIT)", agg_func="MAX", window="100d"),
        ),
        Feature(name="f_days_credit_min", key=loan_id, feature_type=FLOAT,
            transform=WindowAggTransformation(agg_expr="cast_float(DAYS_CREDIT)", agg_func="MIN", window="100d"),
        ),
        # TODO
        #  1. agg var
        #  2. null value
        #  3. cat type
        #  4. not work, bureau_balance has no SK_ID_CURR?

        Feature(name="f_days_credit_enddate_avg", key=loan_id, feature_type=FLOAT,
            transform=WindowAggTransformation(agg_expr="cast_float(NVL(DAYS_CREDIT_ENDDATE,'-1'))", agg_func="AVG", window="100d"),
        ),
        Feature(name="f_days_credit_enddate_min", key=loan_id, feature_type=FLOAT,
            transform=WindowAggTransformation(agg_expr="cast_float(NVL(DAYS_CREDIT_ENDDATE,'-1'))", agg_func="MIN", window="100d"),
        ),
        Feature(name="f_days_credit_enddate_max", key=loan_id, feature_type=FLOAT,
            transform=WindowAggTransformation(agg_expr="cast_float(NVL(DAYS_CREDIT_ENDDATE,'-1'))", agg_func="MAX", window="100d"),
        ),

    ]

    # derived_features = [
    #     DerivedFeature(
    #         name="f_bb_months_balance_min_min",
    #         key=loan_id,
    #         feature_type=FLOAT,
    #         input_features=[f_bb_months_balance_min],
    #         transform=WindowAggTransformation(agg_expr="f_bb_months_balance_min", agg_func="MIN", window="100d"),
    #     ),
    # ]

    application_feature_names = [feat.name for feat in application_features]
    agg_bureau_features_names = [feat.name for feat in agg_bureau_features]
    # agg_bb_features_names = [feat.name for feat in agg_bb_features]
    
    all_feature_names = application_feature_names + agg_bureau_features_names# + agg_bb_features_names
