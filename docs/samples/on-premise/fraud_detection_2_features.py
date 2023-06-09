from feathr import (
    STRING, BOOLEAN, FLOAT, INT32, ValueType,
    Feature, DerivedFeature,
    WindowAggTransformation,
    TypedKey,
)

# Define Features
# - Account features: Account-level features that will be joined to observation data on accountID
# - Transaction features: The features that will be joined to observation data on transactionID
# - Transaction aggregated features: The features aggregated by accountID
# - Derived features: The features derived from other features
# 
class FraudDetectionSchema:
    # Account features will be joined to observation data on accountID
    account_id = TypedKey(
        key_column="accountID",
        key_column_type=ValueType.STRING,
        description="account id",
    )

    account_features = [
        Feature(
            name="account_country_code",
            key=account_id,
            feature_type=STRING, 
            transform="accountCountry",
        ),
        Feature(
            name="is_user_registered",
            key=account_id,
            feature_type=BOOLEAN,
            transform="isUserRegistered==TRUE",
        ),
        Feature(
            name="num_payment_rejects_1d_per_user",
            key=account_id,
            feature_type=INT32,
            transform="numPaymentRejects1dPerUser",
        ),
        Feature(
            name="account_age",
            key=account_id,
            feature_type=INT32,
            transform="accountAge",
        ),
    ]

    # Transaction features will be joined to observation data on transactionID
    transaction_id = TypedKey(
        key_column="transactionID",
        key_column_type=ValueType.STRING,
        description="transaction id",
    )

    transaction_amount = Feature(
        name="transaction_amount",
        key=transaction_id,
        feature_type=FLOAT,
        transform="transactionAmount",
    )

    transaction_features = [
        transaction_amount,
        Feature(
            name="transaction_country_code",
            key=transaction_id,
            feature_type=STRING,
            transform="ipCountryCode",
        ),
        Feature(
            name="transaction_time",
            key=transaction_id,
            feature_type=FLOAT,
            transform="localHour",  # Local time of the transaction
        ),
        Feature(
            name="is_proxy_ip",
            key=transaction_id,
            feature_type=STRING,  # [nan, True, False]
            transform="isProxyIP",
        ),
        Feature(
            name="cvv_verify_result",
            key=transaction_id,
            feature_type=STRING,  # [nan, 'M', 'P', 'N', 'X', 'U', 'S', 'Y']
            transform="cvvVerifyResult",
        ),
    ]

    # ### Define transaction aggregated-features

    # average amount of transaction in that week
    avg_transaction_amount = Feature(
        name="avg_transaction_amount",
        key=account_id,
        feature_type=FLOAT,
        transform=WindowAggTransformation(
            agg_expr="cast_float(transactionAmount)", agg_func="AVG", window="7d"
        ),
    )

    agg_features = [
        avg_transaction_amount,
        # number of transaction that took place in a day
        Feature(
            name="num_transaction_count_in_day",
            key=account_id,
            feature_type=INT32,
            transform=WindowAggTransformation(
                agg_expr="transactionID", agg_func="COUNT", window="1d"
            ),
        ),
        # number of transaction that took place in the past week
        Feature(
            name="num_transaction_count_in_week",
            key=account_id,
            feature_type=INT32,
            transform=WindowAggTransformation(
                agg_expr="transactionID", agg_func="COUNT", window="7d"
            ),
        ),
        # amount of transaction that took place in a day
        Feature(
            name="total_transaction_amount_in_day",
            key=account_id,
            feature_type=FLOAT,
            transform=WindowAggTransformation(
                agg_expr="cast_float(transactionAmount)", agg_func="SUM", window="1d"
            ),
        ),
        # average time of transaction in the past week
        Feature(
            name="avg_transaction_time_in_week",
            key=account_id,
            feature_type=FLOAT,
            transform=WindowAggTransformation(
                agg_expr="cast_float(localHour)", agg_func="AVG", window="7d"
            ),
        ),
    ]

    # ### Define derived features

    derived_features = [
        DerivedFeature(
            name="diff_between_current_and_avg_amount",
            key=[transaction_id, account_id],
            feature_type=FLOAT,
            input_features=[transaction_amount, avg_transaction_amount],
            transform="transaction_amount - avg_transaction_amount",
        ),
    ]

    account_feature_names = [feat.name for feat in account_features] + [feat.name for feat in agg_features]
    transactions_feature_names = [feat.name for feat in transaction_features]
    derived_feature_names = [feat.name for feat in derived_features]

    all_feature_names = account_feature_names + transactions_feature_names + derived_feature_names

    # return (account_id, transaction_id, account_features, transaction_features, agg_features, derived_features)
