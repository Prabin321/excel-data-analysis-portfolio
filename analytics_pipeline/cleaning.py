from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from analytics_pipeline.config import PipelineConfig
from analytics_pipeline.validation import SchemaInference
from analytics_pipeline.logging_utils import get_logger


@dataclass
class TransformationLog:
    actions: list[str] = field(default_factory=list)

    def add(self, message: str) -> None:
        self.actions.append(message)


class DataCleaningService:
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.logger = get_logger(
            self.__class__.__name__, config.output_dir / "logs"
        )

    def clean(
        self,
        df: pd.DataFrame,
        schema: SchemaInference,
    ) -> tuple[pd.DataFrame, TransformationLog]:
        cleaned = df.copy()
        log = TransformationLog()

        for col in schema.numeric:
            cleaned[col] = pd.to_numeric(cleaned[col], errors="coerce")

        cleaned, numeric_log = self._handle_numeric_missing(cleaned, schema.numeric)
        for item in numeric_log:
            log.add(item)

        cleaned, cat_log = self._handle_categorical_missing(cleaned, schema.categorical)
        for item in cat_log:
            log.add(item)

        for col in schema.categorical:
            before = cleaned[col].copy()
            cleaned[col] = (
                cleaned[col].astype(str).str.strip().str.lower().replace({"nan": None})
            )
            if not cleaned[col].equals(before):
                msg = f"Normalized categorical column '{col}' (trim + lowercase)."
                log.add(msg)

        for col in schema.date:
            cleaned[col] = pd.to_datetime(cleaned[col], errors="coerce", utc=True)
            log.add(f"Standardized date column '{col}' to UTC datetime.")

        self.logger.info("Applied %d transformations", len(log.actions))
        return cleaned, log

    def _handle_numeric_missing(
        self,
        df: pd.DataFrame,
        columns: list[str],
    ) -> tuple[pd.DataFrame, list[str]]:
        logs: list[str] = []
        strategy = self.config.missing_numeric_strategy

        for col in columns:
            missing_before = int(df[col].isna().sum())
            if missing_before == 0:
                continue

            if strategy == "mean":
                fill_value = df[col].mean()
                df[col] = df[col].fillna(fill_value)
            elif strategy == "median":
                fill_value = df[col].median()
                df[col] = df[col].fillna(fill_value)
            elif strategy == "mode":
                fill_value = df[col].mode().iloc[0] if not df[col].mode().empty else 0
                df[col] = df[col].fillna(fill_value)
            elif strategy == "constant":
                fill_value = self.config.missing_constant
                df[col] = df[col].fillna(fill_value)
            elif strategy == "drop":
                df = df[df[col].notna()]
                fill_value = "dropped rows"
            else:
                raise ValueError(f"Unsupported numeric missing strategy: {strategy}")

            logs.append(
                f"Numeric missing strategy '{strategy}' on '{col}' ({missing_before} nulls, value={fill_value})."
            )

        return df, logs

    def _handle_categorical_missing(
        self,
        df: pd.DataFrame,
        columns: list[str],
    ) -> tuple[pd.DataFrame, list[str]]:
        logs: list[str] = []
        strategy = self.config.missing_categorical_strategy

        for col in columns:
            missing_before = int(df[col].isna().sum())
            if missing_before == 0:
                continue

            if strategy == "mode":
                mode = df[col].mode()
                fill_value = mode.iloc[0] if not mode.empty else self.config.missing_constant
                df[col] = df[col].fillna(fill_value)
            elif strategy == "constant":
                fill_value = self.config.missing_constant
                df[col] = df[col].fillna(fill_value)
            elif strategy == "drop":
                df = df[df[col].notna()]
                fill_value = "dropped rows"
            else:
                raise ValueError(f"Unsupported categorical missing strategy: {strategy}")

            logs.append(
                f"Categorical missing strategy '{strategy}' on '{col}' ({missing_before} nulls, value={fill_value})."
            )

        return df, logs
