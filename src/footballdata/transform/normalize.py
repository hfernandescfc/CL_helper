from __future__ import annotations

import pandas as pd


def ensure_dtypes_matches(df: pd.DataFrame) -> pd.DataFrame:
    """Optional light Python normalization for raw matches before SQL stage.
    This is a placeholder for adjustments like timestamp parsing, etc.
    """
    return df

