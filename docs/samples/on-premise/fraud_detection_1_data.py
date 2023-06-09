from feathr.datasets.constants import (
    FRAUD_DETECTION_ACCOUNT_INFO_URL,
    FRAUD_DETECTION_FRAUD_TRANSACTIONS_URL,
    FRAUD_DETECTION_UNTAGGED_TRANSACTIONS_URL,
)
from feathr.datasets.utils import maybe_download
from feathr import FeathrClient
import pandas as pd
import numpy as np

# 
# ### Prepare datasets
# 
# We prepare the fraud detection dataset as follows:
# 
# 1. Download Account info data, fraud transactions data, and untagged transactions data.
# 2. Tag transaction data based on the fraud transactions data.
#     1. Aggregate the Fraud table on the account level, creating a start and end datetime. 
#     2. Join this data with the untagged data.
#     3. Tag the data: `is_fraud = 0` for non fraud, `1` for fraud. 
# 3. Upload data files to cloud so that the Feathr's target cluster can consume.
# 
# To learn more about the fraud detection scenario as well as the dataset source we use and the method we tag the transactions, please see [here](https://microsoft.github.io/r-server-fraud-detection/data-scientist.html).

def prepare_data(working_dir: str, client: FeathrClient):
    # Download datasets
    WORKING_DIR = 'fraud_detection' # To use parameter
    account_info_file_path = f"{WORKING_DIR}/account_info.csv"
    fraud_transactions_file_path = f"{WORKING_DIR}/fraud_transactions.csv"
    obs_transactions_file_path = f"{WORKING_DIR}/obs_transactions.csv"
    maybe_download(
        src_url=FRAUD_DETECTION_ACCOUNT_INFO_URL,
        dst_filepath=account_info_file_path,
    )
    maybe_download(
        src_url=FRAUD_DETECTION_FRAUD_TRANSACTIONS_URL,
        dst_filepath=fraud_transactions_file_path,
    )
    maybe_download(
        src_url=FRAUD_DETECTION_UNTAGGED_TRANSACTIONS_URL,
        dst_filepath=obs_transactions_file_path,
    )

    # Load datasets
    fraud_df = pd.read_csv(fraud_transactions_file_path)
    obs_df = pd.read_csv(obs_transactions_file_path)

    # Combine transactionDate and transactionTime into one column. E.g. "20130903", "013641" -> "20130903 013641"
    fraud_df["timestamp"] = fraud_df["transactionDate"].astype(str) + " " + fraud_df["transactionTime"].astype(str).str.zfill(6)
    obs_df["timestamp"] = obs_df["transactionDate"].astype(str) + " " + obs_df["transactionTime"].astype(str).str.zfill(6)


    # In this step, we compute the timestamp range that the frauds were happened by referencing the transaction-level fraud data.
    # We create the labels `is_fraud` to the untagged transaction data based on that.

    # For each user in the fraud transaction data, get the timestamp range that the fraud transactions were happened. 
    fraud_labels_df = fraud_df.groupby("accountID").agg({"timestamp": ['min', 'max']})
    fraud_labels_df.columns = ["_".join(col) for col in fraud_labels_df.columns.values]
    print(fraud_labels_df.head())

    # Combine fraud and untagged transaction data to generate the tagged transaction data.
    transactions_df = pd.concat([fraud_df, obs_df], ignore_index=True).merge(
        fraud_labels_df,
        on="accountID",
        how="outer",
    )

    # Data cleaning
    transactions_df.dropna(
        subset=[
            "accountID",
            "transactionID",
            "transactionAmount",
            "localHour",
            "timestamp",
        ],
        inplace=True,
    )
    transactions_df.sort_values("timestamp", inplace=True)
    transactions_df.drop_duplicates(inplace=True)

    # is_fraud = 0 if the transaction is not fraud. Otherwise (if it is a fraud), is_fraud = 1.
    transactions_df["is_fraud"] = np.logical_and(
        transactions_df["timestamp_min"] <= transactions_df["timestamp"],
        transactions_df["timestamp"] <= transactions_df["timestamp_max"],
    ).astype(int)

    transactions_df.head()

    transactions_df["is_fraud"].value_counts()


    # Save the tagged transaction data into file
    transactions_file_path = f"{WORKING_DIR}/transactions.csv"
    transactions_df.to_csv(transactions_file_path, index=False)

    # upload the local file to the cloud storage (either dbfs or adls).
    account_info_source_path = client.feathr_spark_launcher.upload_or_get_cloud_path(account_info_file_path)
    transactions_source_path = client.feathr_spark_launcher.upload_or_get_cloud_path(transactions_file_path)
    print(account_info_source_path)
    print(transactions_source_path)

    # Check account data
    print(pd.read_csv(account_info_file_path).head())

    return (account_info_source_path, transactions_source_path)
