from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pandas as pd
import streamlit as st

from analytics_pipeline.analysis import AnalysisService
from analytics_pipeline.cleaning import DataCleaningService
from analytics_pipeline.config import PipelineConfig
from analytics_pipeline.ingestion import FileIngestionError, FileIngestionService
from analytics_pipeline.insights import InsightGenerationService
from analytics_pipeline.validation import ValidationService
from analytics_pipeline.visualization import VisualizationService


st.set_page_config(page_title="CSV/Excel Analytics App", layout="wide")


@st.cache_data
def _run_pipeline_bytes(file_name: str, data: bytes) -> dict:
    with tempfile.NamedTemporaryFile(delete=False, suffix=Path(file_name).suffix) as tmp:
        tmp.write(data)
        temp_path = Path(tmp.name)

    config = PipelineConfig(output_dir=Path("outputs_streamlit"))
    config.ensure_output_dirs()

    ingestion_service = FileIngestionService(config)
    validation_service = ValidationService(config)
    cleaning_service = DataCleaningService(config)
    analysis_service = AnalysisService(config)
    viz_service = VisualizationService(config)
    insight_service = InsightGenerationService()

    ingestion = ingestion_service.ingest(temp_path)
    quality = validation_service.build_quality_report(ingestion.dataframe)
    schema = quality.inferred_schema
    cleaned_df, transform_log = cleaning_service.clean(ingestion.dataframe, schema)
    analysis = analysis_service.analyze(cleaned_df, schema)
    charts = viz_service.generate(cleaned_df, schema)
    insights = insight_service.generate(quality, analysis)

    return {
        "raw": ingestion.dataframe,
        "cleaned": cleaned_df,
        "quality": quality,
        "schema": schema,
        "transformations": transform_log.actions,
        "analysis": analysis,
        "charts": charts,
        "insights": insights,
    }


def _apply_sidebar_filters(df: pd.DataFrame, schema_categorical: list[str]) -> pd.DataFrame:
    st.sidebar.header("Filters")
    filtered = df.copy()

    for col in schema_categorical[:5]:
        values = [v for v in filtered[col].dropna().unique().tolist()]
        if not values:
            continue
        selected = st.sidebar.multiselect(f"{col}", options=values, default=values)
        if selected:
            filtered = filtered[filtered[col].isin(selected)]

    return filtered


def _render_insights_block(result: dict) -> None:
    insights = result["insights"]
    quality = result["quality"]
    analysis = result["analysis"]

    st.subheader("Executive Summary")
    st.write(insights.executive_summary)

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**Key Drivers**")
        for driver in insights.key_drivers:
            st.write(f"- {driver}")
        st.markdown("**Risks**")
        for risk in insights.risks:
            st.write(f"- {risk}")

    with col2:
        st.markdown("**Recommended Next Actions**")
        for action in insights.next_actions:
            st.write(f"- {action}")
        st.markdown("**Data Quality Snapshot**")
        st.json(
            {
                "row_count": quality.row_count,
                "column_count": quality.column_count,
                "duplicate_count": quality.duplicate_count,
                "missing_total": sum(quality.missing_counts.values()),
                "outlier_counts": quality.outlier_counts,
            }
        )

    with st.expander("Analysis Details (JSON)"):
        st.code(json.dumps(analysis.__dict__, indent=2, default=str), language="json")


def main() -> None:
    st.title("Production CSV/Excel Analytics Interface")
    st.caption("Upload a CSV/XLS/XLSX file to run ingestion, quality checks, cleaning, analysis, visualization, and insight generation.")

    uploaded = st.file_uploader("Upload data file", type=["csv", "xlsx", "xls"])

    if uploaded is None:
        st.info("Upload a file to begin analysis.")
        return

    try:
        result = _run_pipeline_bytes(uploaded.name, uploaded.getvalue())
    except FileIngestionError as exc:
        st.error(f"File ingestion error: {exc}")
        return
    except Exception as exc:  # noqa: BLE001
        st.error(f"Unexpected failure: {exc}")
        return

    cleaned_df = result["cleaned"]
    schema = result["schema"]

    filtered_df = _apply_sidebar_filters(cleaned_df, schema.categorical)

    st.subheader("Preview")
    st.dataframe(filtered_df.head(200), use_container_width=True)

    st.subheader("Auto-Generated Charts")
    chart_columns = st.columns(2)
    for i, chart_path in enumerate(result["charts"]):
        with chart_columns[i % 2]:
            st.image(str(chart_path), caption=Path(chart_path).name, use_column_width=True)

    st.subheader("Transformation Log")
    for item in result["transformations"]:
        st.write(f"- {item}")

    _render_insights_block(result)


if __name__ == "__main__":
    main()
