from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.metrics import mean_squared_error, r2_score, accuracy_score
from sklearn.model_selection import train_test_split

from analytics_pipeline.config import PipelineConfig
from analytics_pipeline.validation import SchemaInference
from analytics_pipeline.logging_utils import get_logger


@dataclass
class AnalysisResult:
    summary_statistics: dict[str, dict[str, float]]
    correlation_matrix: dict[str, dict[str, float]]
    segment_summaries: dict[str, dict[str, float | int]]
    trend_signals: dict[str, str]
    anomaly_counts: dict[str, int]
    predictive_model_metrics: dict[str, float] | None


class AnalysisService:
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.logger = get_logger(
            self.__class__.__name__, config.output_dir / "logs"
        )

    def analyze(self, df: pd.DataFrame, schema: SchemaInference) -> AnalysisResult:
        summary = df[schema.numeric].describe().to_dict() if schema.numeric else {}
        corr = df[schema.numeric].corr(numeric_only=True).fillna(0).to_dict() if schema.numeric else {}
        segment_summaries = self._segment(df, schema)
        trend_signals = self._detect_trends(df, schema)
        anomaly_counts = self._anomaly_counts(df, schema)
        model_metrics = self._optional_model(df, schema)

        return AnalysisResult(
            summary_statistics=summary,
            correlation_matrix=corr,
            segment_summaries=segment_summaries,
            trend_signals=trend_signals,
            anomaly_counts=anomaly_counts,
            predictive_model_metrics=model_metrics,
        )

    def apply_filters(self, df: pd.DataFrame, filters: dict[str, list | tuple | set]) -> pd.DataFrame:
        filtered = df.copy()
        for column, accepted_values in filters.items():
            if column in filtered.columns:
                filtered = filtered[filtered[column].isin(accepted_values)]
        return filtered

    def _segment(self, df: pd.DataFrame, schema: SchemaInference) -> dict[str, dict[str, float | int]]:
        output: dict[str, dict[str, float | int]] = {}
        if not schema.categorical or not schema.numeric:
            return output

        key_dimension = schema.categorical[0]
        grouped = df.groupby(key_dimension)[schema.numeric].mean(numeric_only=True)
        for key, values in grouped.iterrows():
            output[str(key)] = {k: float(v) for k, v in values.to_dict().items()}
        return output

    def _detect_trends(self, df: pd.DataFrame, schema: SchemaInference) -> dict[str, str]:
        trends: dict[str, str] = {}
        if not schema.date or not schema.numeric:
            return trends

        date_col = schema.date[0]
        ordered = df.sort_values(date_col)
        for metric in schema.numeric:
            series = ordered[metric].dropna().values
            if len(series) < 3:
                trends[metric] = "insufficient data"
                continue
            x = np.arange(len(series))
            slope = np.polyfit(x, series, 1)[0]
            if slope > 0:
                trends[metric] = "upward"
            elif slope < 0:
                trends[metric] = "downward"
            else:
                trends[metric] = "flat"
        return trends

    def _anomaly_counts(self, df: pd.DataFrame, schema: SchemaInference) -> dict[str, int]:
        anomalies: dict[str, int] = {}
        for col in schema.numeric:
            q1, q3 = df[col].quantile([0.25, 0.75])
            iqr = q3 - q1
            if iqr == 0 or pd.isna(iqr):
                anomalies[col] = 0
                continue
            lower = q1 - 1.5 * iqr
            upper = q3 + 1.5 * iqr
            anomalies[col] = int(((df[col] < lower) | (df[col] > upper)).sum())
        return anomalies

    def _optional_model(
        self,
        df: pd.DataFrame,
        schema: SchemaInference,
    ) -> dict[str, float] | None:
        target = self.config.target_column
        if not target or target not in df.columns:
            return None

        features = [c for c in schema.numeric if c != target]
        if len(features) < 1:
            return None

        model_df = df[features + [target]].dropna()
        if len(model_df) < 10:
            return None

        x = model_df[features]
        y = model_df[target]
        x_train, x_test, y_train, y_test = train_test_split(
            x, y, test_size=0.2, random_state=self.config.random_state
        )

        if self.config.model_type == "classification":
            model = LogisticRegression(max_iter=1000)
            model.fit(x_train, y_train)
            preds = model.predict(x_test)
            return {"accuracy": float(accuracy_score(y_test, preds))}

        model = LinearRegression()
        model.fit(x_train, y_train)
        preds = model.predict(x_test)
        return {
            "rmse": float(np.sqrt(mean_squared_error(y_test, preds))),
            "r2": float(r2_score(y_test, preds)),
        }
