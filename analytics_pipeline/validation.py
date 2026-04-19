from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from analytics_pipeline.config import PipelineConfig
from analytics_pipeline.logging_utils import get_logger


@dataclass
class SchemaInference:
    numeric: list[str]
    categorical: list[str]
    date: list[str]


@dataclass
class DataQualityReport:
    row_count: int
    column_count: int
    inferred_schema: SchemaInference
    missing_counts: dict[str, int]
    inconsistent_value_columns: list[str]
    duplicate_count: int
    outlier_counts: dict[str, int]
    empty_dataset: bool


class ValidationService:
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.logger = get_logger(
            self.__class__.__name__, config.output_dir / "logs"
        )

    def infer_schema(self, df: pd.DataFrame) -> SchemaInference:
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()

        remaining = [c for c in df.columns if c not in numeric_cols]
        date_cols: list[str] = []
        categorical_cols: list[str] = []

        for col in remaining:
            parsed = pd.to_datetime(df[col], errors="coerce", utc=True)
            parse_rate = parsed.notna().mean() if len(df) else 0
            if parse_rate >= 0.8:
                date_cols.append(col)
            else:
                categorical_cols.append(col)

        return SchemaInference(
            numeric=numeric_cols,
            categorical=categorical_cols,
            date=date_cols,
        )

    def build_quality_report(self, df: pd.DataFrame) -> DataQualityReport:
        schema = self.infer_schema(df)
        missing_counts = df.isna().sum().astype(int).to_dict()
        inconsistent = self._find_inconsistent_columns(df, schema.categorical)
        duplicate_count = int(df.duplicated(subset=self.config.duplicate_subset).sum())
        outlier_counts = self._detect_outliers(df, schema.numeric)
        report = DataQualityReport(
            row_count=len(df),
            column_count=len(df.columns),
            inferred_schema=schema,
            missing_counts=missing_counts,
            inconsistent_value_columns=inconsistent,
            duplicate_count=duplicate_count,
            outlier_counts=outlier_counts,
            empty_dataset=df.empty,
        )
        self.logger.info("Built quality report: %s", report)
        return report

    def _find_inconsistent_columns(
        self,
        df: pd.DataFrame,
        categorical_columns: list[str],
    ) -> list[str]:
        inconsistent_columns: list[str] = []
        for col in categorical_columns:
            values = df[col].dropna().astype(str)
            if values.empty:
                continue
            normalized = values.str.strip().str.lower()
            if normalized.nunique() < values.nunique():
                inconsistent_columns.append(col)
        return inconsistent_columns

    def _detect_outliers(
        self,
        df: pd.DataFrame,
        numeric_columns: list[str],
    ) -> dict[str, int]:
        outlier_counts: dict[str, int] = {}
        threshold = self.config.zscore_threshold

        for col in numeric_columns:
            series = df[col].dropna()
            if len(series) < 3 or series.std(ddof=0) == 0:
                outlier_counts[col] = 0
                continue
            z_scores = (series - series.mean()) / series.std(ddof=0)
            outlier_counts[col] = int((z_scores.abs() > threshold).sum())

        return outlier_counts
