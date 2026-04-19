from __future__ import annotations

import argparse

from analytics_pipeline.config import PipelineConfig
from analytics_pipeline.pipeline import AnalyticsPipeline


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run analytics pipeline on CSV/Excel files")
    parser.add_argument("input_file", help="Path to CSV/XLSX input")
    parser.add_argument("--output-dir", default="outputs", help="Directory for reports/charts/logs")
    parser.add_argument("--max-file-size-mb", type=int, default=100)
    parser.add_argument("--target-column", default=None)
    parser.add_argument("--model-type", choices=["regression", "classification"], default="regression")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    config = PipelineConfig(
        output_dir=args.output_dir,
        max_file_size_mb=args.max_file_size_mb,
        target_column=args.target_column,
        model_type=args.model_type,
    )
    pipeline = AnalyticsPipeline(config)
    result = pipeline.run(args.input_file)
    print("Pipeline run complete")
    print(result.insights.executive_summary)


if __name__ == "__main__":
    main()
