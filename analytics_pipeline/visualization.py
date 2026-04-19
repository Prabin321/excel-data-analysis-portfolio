from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from analytics_pipeline.config import PipelineConfig
from analytics_pipeline.validation import SchemaInference
from analytics_pipeline.logging_utils import get_logger


class VisualizationService:
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.logger = get_logger(
            self.__class__.__name__, config.output_dir / "logs"
        )

    def generate(self, df: pd.DataFrame, schema: SchemaInference) -> list[Path]:
        chart_paths: list[Path] = []
        chart_dir = self.config.output_dir / "charts"
        chart_dir.mkdir(parents=True, exist_ok=True)

        if df.empty:
            self.logger.warning("Dataset is empty; skipping chart generation")
            return chart_paths

        if schema.numeric:
            col = schema.numeric[0]
            fig, ax = plt.subplots(figsize=(8, 4))
            bins = 5 if len(df) < 30 else 20
            ax.hist(df[col].dropna(), bins=bins)
            ax.set_title(f"Distribution of {col}")
            ax.set_xlabel(col)
            ax.set_ylabel("Frequency")
            path = chart_dir / f"hist_{col}.png"
            fig.tight_layout()
            fig.savefig(path)
            plt.close(fig)
            chart_paths.append(path)

        if len(schema.numeric) >= 2:
            x_col, y_col = schema.numeric[:2]
            fig, ax = plt.subplots(figsize=(8, 4))
            ax.scatter(df[x_col], df[y_col], alpha=0.7)
            ax.set_title(f"{y_col} vs {x_col}")
            ax.set_xlabel(x_col)
            ax.set_ylabel(y_col)
            path = chart_dir / f"scatter_{x_col}_{y_col}.png"
            fig.tight_layout()
            fig.savefig(path)
            plt.close(fig)
            chart_paths.append(path)

        if schema.categorical:
            col = schema.categorical[0]
            counts = df[col].value_counts().head(10)
            fig, ax = plt.subplots(figsize=(8, 4))
            counts.plot(kind="bar", ax=ax)
            ax.set_title(f"Top categories in {col}")
            ax.set_xlabel(col)
            ax.set_ylabel("Count")
            path = chart_dir / f"bar_{col}.png"
            fig.tight_layout()
            fig.savefig(path)
            plt.close(fig)
            chart_paths.append(path)

        if schema.date and schema.numeric:
            date_col = schema.date[0]
            metric = schema.numeric[0]
            grouped = (
                df[[date_col, metric]].dropna().sort_values(date_col).set_index(date_col).resample("D").mean()
            )
            if not grouped.empty:
                fig, ax = plt.subplots(figsize=(8, 4))
                grouped[metric].plot(ax=ax)
                ax.set_title(f"Trend of {metric} over time")
                ax.set_xlabel("Date")
                ax.set_ylabel(metric)
                path = chart_dir / f"trend_{metric}.png"
                fig.tight_layout()
                fig.savefig(path)
                plt.close(fig)
                chart_paths.append(path)

        return chart_paths
