from pathlib import Path

from uk_travel_pipeline.cli import build_parser


def test_cli_supports_explicit_api_and_filter_paths():
    parser = build_parser()
    args = parser.parse_args(
        [
            "run",
            "--api-data-path",
            "custom/api_data.parquet",
            "--msoa-filter-list-path",
            "custom/filter.csv",
        ]
    )
    assert args.bt_parquet == Path("custom/api_data.parquet")
    assert args.msoa_filter_list == Path("custom/filter.csv")


def test_cli_keeps_legacy_aliases_for_paths():
    parser = build_parser()
    args = parser.parse_args(
        [
            "run",
            "--bt-parquet",
            "legacy/api_data.parquet",
            "--msoa-filter-list",
            "legacy/filter.csv",
        ]
    )
    assert args.bt_parquet == Path("legacy/api_data.parquet")
    assert args.msoa_filter_list == Path("legacy/filter.csv")


def test_cli_supports_skip_child_origin_uplift():
    parser = build_parser()
    args = parser.parse_args(["reassign", "--skip-child-origin-uplift"])
    assert args.skip_child_origin_uplift is True


def test_cli_supports_population_csv_alias():
    parser = build_parser()
    args = parser.parse_args(["run", "--population-csv", "custom/pop_id.csv"])
    assert args.pop_lsoa_internal_csv == Path("custom/pop_id.csv")


def test_cli_supports_skip_qa_summary_for_matrices():
    parser = build_parser()
    args = parser.parse_args(["matrices", "--skip-qa-summary"])
    assert args.skip_qa_summary is True


def test_cli_supports_local_authority_use_case_options():
    parser = build_parser()
    args = parser.parse_args(
        [
            "local-authority-use-case",
            "--output-dir",
            "custom/local_authority",
            "--top-n",
            "12",
        ]
    )
    assert args.output_dir == Path("custom/local_authority")
    assert args.top_n == 12


def test_cli_supports_local_authority_visualisation_options():
    parser = build_parser()
    args = parser.parse_args(
        [
            "local-authority-visuals",
            "--input-dir",
            "custom/local_authority",
            "--output-dir",
            "custom/visuals",
            "--top-n",
            "8",
        ]
    )
    assert args.input_dir == Path("custom/local_authority")
    assert args.output_dir == Path("custom/visuals")
    assert args.top_n == 8
