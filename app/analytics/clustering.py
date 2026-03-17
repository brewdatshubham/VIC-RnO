from __future__ import annotations

import numpy as np
import pandas as pd


def _kmeans_1d(x: np.ndarray, k: int, seed: int = 42, max_iter: int = 100) -> tuple[np.ndarray, np.ndarray]:
    """
    Minimal 1D k-means (numpy only), returns (labels, centers).
    centers are in raw value space.
    """
    x = np.asarray(x, dtype=float).reshape(-1)
    n = len(x)
    if n == 0:
        return np.array([], dtype=int), np.array([], dtype=float)

    k = int(max(1, min(k, n)))
    rng = np.random.default_rng(seed)
    centers = x[rng.choice(n, size=k, replace=False)].copy()

    labels = np.zeros(n, dtype=int)
    for _ in range(max_iter):
        # assign
        d = np.abs(x[:, None] - centers[None, :])  # (n, k)
        new_labels = d.argmin(axis=1)
        if np.array_equal(new_labels, labels):
            break
        labels = new_labels
        # update
        for i in range(k):
            mask = labels == i
            if mask.any():
                centers[i] = x[mask].mean()

    return labels, centers


def cluster_volume_1d(volumes: np.ndarray, n_clusters: int = 3, seed: int = 42) -> tuple[np.ndarray, np.ndarray]:
    """
    Cluster 1D volumes and remap labels so 0=smallest center, 1=next, ...
    Returns (labels_sorted, centers_sorted).
    """
    labels, centers = _kmeans_1d(volumes, k=n_clusters, seed=seed)
    if centers.size == 0:
        return labels, centers

    order = np.argsort(centers)
    old_to_new = {int(old): int(new) for new, old in enumerate(order.tolist())}
    labels_sorted = np.vectorize(old_to_new.get)(labels)
    centers_sorted = centers[order]
    return labels_sorted, centers_sorted


def cluster_plants_from_vilc(
    df: pd.DataFrame,
    plant_col: str = "Entity_1",
    volume_col: str = "MTH_Volume",
    n_clusters: int = 3,
    seed: int = 42,
) -> pd.DataFrame:
    """
    Aggregate to plant-level and cluster plants by volume.
    Returns: DataFrame with columns [Entity_1, MTH_Volume, Cluster]
    """
    if plant_col not in df.columns:
        return pd.DataFrame()
    if volume_col not in df.columns:
        return pd.DataFrame()

    plant_agg = df.groupby(plant_col, as_index=False)[volume_col].sum()
    vols = plant_agg[volume_col].to_numpy()
    labels, centers = cluster_volume_1d(vols, n_clusters=n_clusters, seed=seed)
    plant_agg["Cluster"] = labels
    plant_agg["ClusterLabel"] = "Cluster " + plant_agg["Cluster"].astype(str)

    # Attach center volume per cluster for display/debugging
    if centers.size:
        center_map = {i: float(c) for i, c in enumerate(centers.tolist())}
        plant_agg["ClusterCenterVolume"] = plant_agg["Cluster"].map(center_map)
    return plant_agg.sort_values([ "Cluster", volume_col], ascending=[True, False]).reset_index(drop=True)

