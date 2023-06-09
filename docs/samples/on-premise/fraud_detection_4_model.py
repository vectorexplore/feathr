
# ## 5. Build a Fraud Detection Model
# 
# We use [Random Forest Classifier](https://scikit-learn.org/stable/modules/generated/sklearn.ensemble.RandomForestClassifier.html) to build a fraud detection model.

from plotly.subplots import make_subplots
import plotly.graph_objects as go
import plotly.express as px
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    PrecisionRecallDisplay,
)
from sklearn.preprocessing import OneHotEncoder, OrdinalEncoder
import numpy as np
import pandas as pd

def train(df):

    # ### Understand the dataset

    df.describe().T

    print(df.nunique())


    # plot only sub-samples for simplicity
    NUM_SAMPLES_TO_PLOT = 5000

    fig = px.scatter_matrix(
        df.sample(n=NUM_SAMPLES_TO_PLOT, random_state=42),
        dimensions=df.columns[:-2],  # exclude the label and timestamp
        color="is_fraud",
        labels={col:col.replace('_', ' ') for col in df.columns}, # remove underscore
    )
    fig.update_traces(diagonal_visible=False, showupperhalf=False, marker_size=3, marker_opacity=0.5)
    fig.update_layout(
        width=2000,
        height=2000,
        title={"text": "Scatter matrix for transaction dataset", "font_size": 20},
        font_size=6,
    )
    fig.show()


    # ### Split training and validation sets

    n_train = int(len(df) * 0.7)

    train_df = df.iloc[:n_train]
    test_df = df.iloc[n_train:]

    print(f"""Training set:
    {train_df["is_fraud"].value_counts()}

    Validation set:
    {test_df["is_fraud"].value_counts()}
    """)

    # Check the time range of the training and test set doesn't overlap
    print(train_df["timestamp"].max(), test_df["timestamp"].min())


    # ### Train and test a machine learning model

    # Get labels as integers
    y_train = train_df["is_fraud"].astype(int).to_numpy()
    y_test = test_df["is_fraud"].astype(int).to_numpy()

    # We convert categorical features into integer values by using one-hot-encoding and ordinal-encoding
    categorical_feature_names = [
        "account_country_code",
        "transaction_country_code",
        "cvv_verify_result",
    ]
    ordinal_feature_names = [
        "is_user_registered",
        "is_proxy_ip",
    ]

    one_hot_encoder = OneHotEncoder(sparse_output=False).fit(df[categorical_feature_names])
    ordinal_encoder = OrdinalEncoder().fit(df[ordinal_feature_names])

    print(ordinal_encoder.categories_)
    print(one_hot_encoder.categories_)

    X_train = np.concatenate(
        (
            one_hot_encoder.transform(train_df[categorical_feature_names]),
            ordinal_encoder.transform(train_df[ordinal_feature_names]),
            train_df.drop(categorical_feature_names + ordinal_feature_names + ["is_fraud", "timestamp"], axis="columns").fillna(0).to_numpy(),
        ),
        axis=1,
    )

    X_test = np.concatenate(
        (
            one_hot_encoder.transform(test_df[categorical_feature_names]),
            ordinal_encoder.transform(test_df[ordinal_feature_names]),
            test_df.drop(categorical_feature_names + ordinal_feature_names + ["is_fraud", "timestamp"], axis="columns").fillna(0).to_numpy(),
        ),
        axis=1,
    )

    clf = RandomForestClassifier(
        n_estimators=50,
        random_state=42,
    ).fit(X_train, y_train)

    print(clf.score(X_test, y_test))

    y_pred = clf.predict(X_test)
    print(y_pred)

    y_prob = clf.predict_proba(X_test)
    print(y_prob)


    # To measure the performance, we use recall, precision and F1 score that handle imbalanced data better.
    display = PrecisionRecallDisplay.from_predictions(
        y_test, y_prob[:, 1], name="RandomForestClassifier"
    )
    _ = display.ax_.set_title("Fraud Detection Precision-Recall Curve")

    precision = precision_score(y_test, y_pred)
    recall = recall_score(y_test, y_pred)
    f1 = f1_score(y_test, y_pred)

    print(f"""Precision: {precision},
    Recall: {recall},
    F1: {f1}""")

    print(confusion_matrix(y_test, y_pred))


    # ### Feature importance
    numeric_feature_names = [name for name in train_df.columns if name not in set(categorical_feature_names + ordinal_feature_names + ["is_fraud", "timestamp"])]
    print(numeric_feature_names)

    # the order of features is [categorical features, ordinal features, numeric features]
    importances = clf.feature_importances_[-len(numeric_feature_names):]
    std = np.std([tree.feature_importances_[-len(numeric_feature_names):] for tree in clf.estimators_], axis=0)

    fig = px.bar(
        pd.DataFrame([numeric_feature_names, importances, std], index=["Numeric features", "importances", "std"]).T,
        y="Numeric features",
        x="importances",
        error_x="std",
        orientation="h",
        title="Importance of the numeric features",
    )
    fig.update_layout(showlegend=False, width=1000)
    fig.update_xaxes(title_text="Mean decrease in impurity", range=[0, 0.5])
    fig.update_yaxes(title_text="Numeric features")
    fig.show()


    feature_names = categorical_feature_names + ordinal_feature_names
    categories = one_hot_encoder.categories_ + ordinal_encoder.categories_

    start_i = 0
    n_rows = len(feature_names)

    fig = make_subplots(
        rows=n_rows,
        cols=1,
        subplot_titles=[name.replace("_", " ") for name in feature_names],
        x_title="Mean decrease in impurity",
    )

    for i in range(n_rows):
        category = categories[i]
        end_i = start_i + len(category)

        fig.add_trace(
            go.Bar(
                x=clf.feature_importances_[start_i:end_i],
                y=category,
                width=0.2,
                error_x=dict(
                    type="data",
                    array=np.std([tree.feature_importances_[start_i:end_i] for tree in clf.estimators_], axis=0),
                ),
                orientation="h",
            ),
            row=i+1,
            col=1,
        )

        start_i = end_i
        
    fig.update_layout(title="Importance of the categorical features", showlegend=False, width=1000, height=1000)
    fig.update_xaxes(range=[0, 0.5])
    fig.show()
