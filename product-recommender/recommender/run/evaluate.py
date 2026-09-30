"""Compares six recommenders offline on the click history, with mab2rec's benchmark.

Each is trained on the first 80% of the history and scored on the rest.

Run: python -m recommender.run.evaluate [--seed 1237]
"""

import argparse
import warnings

import pandas as pd
from jurity.recommenders import BinaryRecoMetrics, RankingRecoMetrics
from mab2rec import BanditRecommender, LearningPolicy, NeighborhoodPolicy
from mab2rec.pipeline import benchmark
from sklearn.model_selection import train_test_split

from recommender.core.config import ALPHA, PRODUCT_FEATURES, SEED, TOP_K, TRAINING_LOG

_UID, _PID, _RESPONSE = "event_id", "product_id", "response"


def _ids(series: pd.Series) -> pd.Series:
    """Returns the ids as strings of integers, with -1 for a missing id."""
    return pd.to_numeric(series, errors="coerce").fillna(-1).astype(int).astype(str)


def _numeric(df: pd.DataFrame, id_col: str) -> pd.DataFrame:
    """Returns the id column and the numeric columns, dropping text ones."""
    columns = df.select_dtypes(include=["number", "bool"]).columns.tolist()
    if id_col not in columns:
        columns.insert(0, id_col)
    return df[columns]


def main() -> None:
    """Prints each recommender's AUC, click rate, precision and recall at 5."""
    parser = argparse.ArgumentParser(description="Compare recommenders offline.")
    parser.add_argument("--seed", type=int, default=SEED, help="Random seed.")
    args = parser.parse_args()
    warnings.simplefilter(action="ignore", category=FutureWarning)
    warnings.simplefilter(action="ignore", category=UserWarning)

    log = pd.read_csv(TRAINING_LOG)
    items = pd.read_csv(PRODUCT_FEATURES)
    log[_UID] = _ids(log[_UID])
    log[_PID] = _ids(log[_PID])
    items[_PID] = _ids(items[_PID])
    # Each event is its own "user", described by the event's user and time features.
    # The product and the response are left out, since they are what is predicted.
    contexts = _numeric(
        log[[c for c in log.columns if c not in [_PID, _RESPONSE]]].copy(), _UID
    )
    items = _numeric(items, _PID)
    train, test = train_test_split(log, test_size=0.2, shuffle=False)  # by time

    # benchmark() renames the response column to "score" before passing it to Jurity.
    params = {
        "click_column": "score",
        "user_id_column": _UID,
        "item_id_column": _PID,
        "k": TOP_K,
    }
    metrics = [
        BinaryRecoMetrics.AUC(**params),
        BinaryRecoMetrics.CTR(**params),
        RankingRecoMetrics.Precision(**params),
        RankingRecoMetrics.Recall(**params),
    ]
    kwargs = {"top_k": TOP_K, "seed": args.seed}
    candidates = {
        "Random": BanditRecommender(LearningPolicy.Random(), **kwargs),
        "Popularity": BanditRecommender(LearningPolicy.Popularity(), **kwargs),
        "LinGreedy": BanditRecommender(LearningPolicy.LinGreedy(epsilon=0.1), **kwargs),
        "LinUCB": BanditRecommender(LearningPolicy.LinUCB(alpha=ALPHA), **kwargs),
        "LinTS": BanditRecommender(LearningPolicy.LinTS(), **kwargs),
        "ClustersTS": BanditRecommender(
            LearningPolicy.ThompsonSampling(),
            NeighborhoodPolicy.Clusters(n_clusters=10),
            **kwargs,
        ),
    }

    print("Running Benchmark... (This trains and scores all models automatically)")
    _, reco_to_metrics = benchmark(
        candidates,
        metrics=metrics,
        train_data=train,
        test_data=test,
        user_features=contexts,
        item_features=items,
        user_id_col=_UID,
        item_id_col=_PID,
        response_col=_RESPONSE,
    )
    print("-" * 80)
    results = pd.DataFrame(reco_to_metrics).T  # one row per recommender
    print("Available Metrics:", results.columns.tolist())
    print(results)
    print("-" * 80)


if __name__ == "__main__":
    main()
