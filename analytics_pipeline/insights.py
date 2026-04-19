from __future__ import annotations

from dataclasses import dataclass

from analytics_pipeline.analysis import AnalysisResult
from analytics_pipeline.validation import DataQualityReport


@dataclass
class InsightReport:
    executive_summary: str
    key_drivers: list[str]
    risks: list[str]
    next_actions: list[str]


class InsightGenerationService:
    def generate(
        self,
        quality: DataQualityReport,
        analysis: AnalysisResult,
    ) -> InsightReport:
        key_drivers: list[str] = []
        risks: list[str] = []
        next_actions: list[str] = []

        if analysis.correlation_matrix:
            strongest = self._find_strongest_correlation(analysis.correlation_matrix)
            if strongest:
                key_drivers.append(
                    f"Strongest correlation observed between {strongest[0]} and {strongest[1]} ({strongest[2]:.2f})."
                )

        for metric, trend in analysis.trend_signals.items():
            if trend in {"upward", "downward"}:
                key_drivers.append(f"{metric} shows a {trend} trajectory.")

        if quality.duplicate_count > 0:
            risks.append(f"Detected {quality.duplicate_count} duplicate records.")
        missing_total = sum(quality.missing_counts.values())
        if missing_total > 0:
            risks.append(f"Detected {missing_total} missing values across dataset.")

        for col, cnt in analysis.anomaly_counts.items():
            if cnt > 0:
                risks.append(f"Metric '{col}' has {cnt} anomaly candidates.")

        if not key_drivers:
            key_drivers.append("No dominant drivers detected due to limited signal.")

        next_actions.append("Prioritize data quality fixes for missing/duplicate records.")
        next_actions.append("Review anomaly rows with domain owners before operational decisions.")
        next_actions.append("Operationalize this pipeline as a scheduled batch with alerting thresholds.")

        executive_summary = (
            f"Dataset contains {quality.row_count} rows and {quality.column_count} columns. "
            f"Quality checks found {quality.duplicate_count} duplicates and "
            f"{sum(quality.missing_counts.values())} missing values."
        )

        return InsightReport(
            executive_summary=executive_summary,
            key_drivers=key_drivers,
            risks=risks,
            next_actions=next_actions,
        )

    def _find_strongest_correlation(
        self,
        corr: dict[str, dict[str, float]],
    ) -> tuple[str, str, float] | None:
        best_pair: tuple[str, str, float] | None = None
        for a, row in corr.items():
            for b, value in row.items():
                if a == b:
                    continue
                abs_value = abs(value)
                if best_pair is None or abs_value > abs(best_pair[2]):
                    best_pair = (a, b, value)
        return best_pair
